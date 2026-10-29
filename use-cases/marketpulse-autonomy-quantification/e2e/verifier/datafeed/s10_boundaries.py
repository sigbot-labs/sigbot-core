import asyncio
import json

import httpx
import websockets
from a2a.client import A2AClient
from a2a.types import MessageSendParams, SendMessageRequest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client


class BoundaryVerifier:
    @staticmethod
    async def run(context):
        evidence = {}
        async with httpx.AsyncClient(timeout=context.timeout_seconds) as http:
            for provider in ("twitter", "truthsocial"):
                endpoint = context.endpoint(provider)
                path = "/2/users/2244994945/tweets" if provider == "twitter" else "/api/v1/accounts/100000000000001/statuses"
                denied = await http.get(endpoint + path)
                assert denied.status_code == 401, "prepaid API token must be enforced"
                response = await http.get(endpoint + path, headers=context.headers(),
                                          params={"limit": 40} if provider == "truthsocial" else {})
                response.raise_for_status()
                value = response.json()
                rest_posts = value["data"] if isinstance(value, dict) else value
                assert len(rest_posts) >= 3
                assert response.headers["traceparent"] == context.headers()["traceparent"]
                async with httpx.AsyncClient(headers=context.headers(), timeout=context.timeout_seconds) as mcp_http:
                    async with streamable_http_client(endpoint + "/mcp", http_client=mcp_http) as (read, write, _):
                        async with ClientSession(read, write) as session:
                            await session.initialize()
                            tools = await session.list_tools()
                            assert any(tool.name == "timeline" for tool in tools.tools)
                            result = await session.call_tool("timeline", {"limit": 100 if provider == "twitter" else 40})
                            assert not result.isError
                            mcp_posts = (result.structuredContent or json.loads(result.content[0].text))["data"]
                async with httpx.AsyncClient(headers=context.headers(), timeout=context.timeout_seconds) as a2a_http:
                    client = A2AClient(a2a_http, url=endpoint + "/a2a/")
                    request = SendMessageRequest(id=1, params=MessageSendParams.model_validate({"message": {
                        "kind": "message", "role": "user", "messageId": context.run_id,
                        "parts": [{"kind": "text", "text": json.dumps({"limit": 100 if provider == "twitter" else 40})}]}}))
                    result = (await client.send_message(request)).model_dump(mode="json")
                    assert "error" not in result, result
                    a2a_posts = json.loads(result["result"]["parts"][0]["text"])["data"]
                ws_url = endpoint.replace("http://", "ws://").replace("https://", "wss://") + "/ws"
                async with websockets.connect(ws_url, additional_headers=context.headers()) as socket:
                    streamed = [json.loads(await asyncio.wait_for(socket.recv(), context.timeout_seconds)) for _ in range(3)]
                expected = [post["id"] for post in rest_posts]
                assert list(map(int, expected)) == sorted(map(int, expected), reverse=True)
                first = await http.get(endpoint + path, headers=context.headers(),
                                       params={"max_results": 5} if provider == "twitter" else {"limit": 1})
                first.raise_for_status()
                if provider == "twitter":
                    page = first.json()
                    assert page["meta"]["newest_id"] == page["data"][0]["id"]
                    second = await http.get(endpoint + path, headers=context.headers(),
                                            params={"max_results": 5, "pagination_token": page["meta"]["next_token"]})
                    second.raise_for_status()
                    assert second.json()["data"][0]["id"] == expected[5]
                else:
                    assert "next" in first.links, "native Mastodon pagination must expose Link rel=next"
                    second = await http.get(first.links["next"]["url"], headers=context.headers())
                    second.raise_for_status()
                    assert int(second.json()[0]["id"]) < int(first.json()[0]["id"])
                assert expected == [post["id"] for post in mcp_posts] == [post["id"] for post in a2a_posts]
                assert expected[:3] == [post["id"] for post in streamed]
                evidence[provider] = {"rest": len(rest_posts), "mcp": len(mcp_posts), "a2a": len(a2a_posts), "ws": len(streamed)}
            completion = await http.post(context.endpoint("openai") + "/v1/chat/completions", headers=context.headers(),
                json={"model": "marketpulse-fixture", "messages": [{"role": "user", "content": json.dumps(rest_posts)}]})
            completion.raise_for_status()
            evaluation = json.loads(completion.json()["choices"][0]["message"]["content"])
            assert abs(sum(evaluation["probabilities"].values()) - 1.0) < 1e-9
            assert set(evaluation["probabilities"]) == {"trend", "range", "stabilizing", "suppression", "unknown"}
            assert evaluation["facts"] and evaluation["inferences"]
            evidence["openai"] = {"probabilities": evaluation["probabilities"], "facts_separate": True}
        return evidence


def verify(context):
    return asyncio.run(BoundaryVerifier.run(context))
