"""External social API boundaries; MCP and A2A use their upstream Python SDKs."""
from __future__ import annotations

import asyncio
import hmac
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path

from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.apps import A2AStarletteApplication
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore
from a2a.types import AgentCapabilities, AgentCard, AgentSkill
from a2a.utils import new_agent_text_message
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from common.news.fixtures import TimelineFixtures


class TimelineStore:
    def __init__(self, fixtures: TimelineFixtures) -> None:
        self.fixtures = fixtures
        self.posts = fixtures.create()
        self.requests = {"rest": 0, "ws": 0, "mcp": 0, "a2a": 0}
        self.clients: set[asyncio.Queue] = set()

    def timeline(self, mode: str, since_id: str = "0", limit: int = 100, cursor: str = "0", max_id: str | None = None) -> dict:
        if (not 1 <= limit <= 100 or not since_id.isdecimal() or not cursor.isdecimal()
                or (max_id is not None and not max_id.isdecimal())):
            raise ValueError("invalid bounded timeline query")
        self.requests[mode] += 1
        selected = sorted((p for p in self.posts if int(p["id"]) > int(since_id)
                           and (max_id is None or int(p["id"]) < int(max_id))),
                          key=lambda post: int(post["id"]), reverse=True)
        offset = int(cursor)
        page = selected[offset:offset + limit]
        next_token = str(offset + limit) if offset + limit < len(selected) else None
        return {"provider": self.fixtures.provider, "data": page, "next_token": next_token}

    async def append(self, posts: list[dict]) -> int:
        if len(posts) > 100:
            raise ValueError("batch exceeds 100")
        for post in posts:
            self.fixtures.validate_post(post)
        self.posts.extend(posts)
        self.posts = self.posts[-10_000:]
        for queue in tuple(self.clients):
            for post in posts:
                try:
                    queue.put_nowait(post)
                except asyncio.QueueFull:
                    # Force reconnect and REST catch-up instead of silently losing data.
                    self.clients.discard(queue)
                    while not queue.empty():
                        queue.get_nowait()
                    queue.put_nowait(None)
                    break
        return len(posts)


class TimelineAgent(AgentExecutor):
    def __init__(self, store: TimelineStore) -> None:
        self.store = store

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        query = json.loads(context.get_user_input())
        result = self.store.timeline("a2a", **query)
        await event_queue.enqueue_event(new_agent_text_message(json.dumps(result)))

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        raise ValueError("timeline reads return a Message; no cancellable task exists")


class SocialService:
    def __init__(self, provider: str) -> None:
        root = Path(os.environ.get("MARKETPULSE_CONFIG_ROOT", Path(__file__).parents[2] / "config"))
        self.store = TimelineStore(TimelineFixtures(provider, root, os.environ.get("MARKETPULSE_FIXTURE_BASE_TIME")))
        self.token = os.environ.get("MARKETPULSE_MOCK_TOKEN", "")
        if not self.token:
            raise ValueError("MARKETPULSE_MOCK_TOKEN is required")
        self.mcp = FastMCP(f"marketpulse-{provider}", json_response=True, stateless_http=True,
                           transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
        self.mcp.add_tool(self.timeline, name="timeline", description="Read a bounded synthetic news timeline")
        self.app = FastAPI(title=f"Mock {provider} boundary", lifespan=self.lifespan)
        self.app.middleware("http")(self.authorize)
        self.app.add_api_route("/healthz", self.health)
        self.app.add_api_route("/stats", self.stats)
        self.app.add_api_route("/fixtures", self.append, methods=["POST"])
        native_path = "/2/users/{account_id}/tweets" if provider == "twitter" else "/api/v1/accounts/{account_id}/statuses"
        self.app.add_api_route(native_path, self.rest)
        self.app.add_api_websocket_route("/ws", self.stream)
        card = AgentCard(name=f"{provider} timeline", description="Synthetic external timeline gateway",
                         url=os.environ.get("MARKETPULSE_PUBLIC_URL", "http://127.0.0.1:8080") + "/a2a/",
                         version="1.0.0", capabilities=AgentCapabilities(),
                         default_input_modes=["text/plain"], default_output_modes=["text/plain"],
                         skills=[AgentSkill(id="timeline", name="Timeline", description="Read timeline", tags=["news"])])
        a2a = A2AStarletteApplication(card, DefaultRequestHandler(TimelineAgent(self.store), InMemoryTaskStore()))
        self.app.mount("/a2a", a2a.build())
        self.app.mount("/", self.mcp.streamable_http_app())

    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        async with self.mcp.session_manager.run():
            yield

    async def authorize(self, request: Request, call_next):
        if request.url.path not in {"/healthz", "/openapi.json", "/docs"}:
            supplied = request.headers.get("authorization", "")
            if not hmac.compare_digest(supplied, f"Bearer {self.token}"):
                return JSONResponse({"error": "unauthorized"}, status_code=401)
        response = await call_next(request)
        if traceparent := request.headers.get("traceparent"):
            response.headers["traceparent"] = traceparent
        return response

    async def health(self) -> dict:
        return {"status": "ok", "provider": self.store.fixtures.provider, "synthetic": True}

    async def stats(self) -> dict:
        return {"requests": self.store.requests, "posts": len(self.store.posts), "websocket_clients": len(self.store.clients)}

    async def append(self, request: Request) -> dict:
        return {"accepted": await self.store.append((await request.json())["data"])}

    async def timeline(self, since_id: str = "0", limit: int = 100, cursor: str = "0") -> dict:
        return self.store.timeline("mcp", since_id, limit, cursor)

    async def rest(self, request: Request, account_id: str, since_id: str = "0", max_results: int = 100,
                   limit: int = 20, pagination_token: str = "0", max_id: str | None = None) -> JSONResponse:
        try:
            twitter = self.store.fixtures.provider == "twitter"
            if twitter and not 5 <= max_results <= 100:
                raise ValueError("X max_results must be between 5 and 100")
            if not twitter and not 1 <= limit <= 40:
                raise ValueError("Mastodon status limit must be between 1 and 40")
            page = self.store.timeline("rest", since_id, max_results if twitter else min(limit, 40),
                                       pagination_token if twitter else "0", None if twitter else max_id)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        headers = {}
        if not twitter and page["next_token"]:
            next_url = request.url.include_query_params(max_id=page["data"][-1]["id"])
            headers["Link"] = f'<{next_url}>; rel="next"'
        return JSONResponse(self.store.fixtures.response(page["data"], page["next_token"]), headers=headers)

    async def stream(self, socket: WebSocket) -> None:
        if not hmac.compare_digest(socket.headers.get("authorization", ""), f"Bearer {self.token}"):
            await socket.close(code=1008)
            return
        await socket.accept()
        queue: asyncio.Queue = asyncio.Queue(maxsize=128)
        self.store.clients.add(queue)
        receive = asyncio.create_task(socket.receive())
        pending = None
        try:
            for post in self.store.timeline("ws")["data"]:
                await socket.send_json(post)
            while True:
                pending = asyncio.create_task(queue.get())
                completed, _ = await asyncio.wait((receive, pending), return_when=asyncio.FIRST_COMPLETED)
                if receive in completed:
                    if receive.result()["type"] == "websocket.disconnect":
                        return
                    receive = asyncio.create_task(socket.receive())
                    if pending not in completed:
                        pending.cancel()
                        continue
                post = pending.result()
                if post is None:
                    await socket.close(code=1013)
                    return
                await socket.send_json(post)
        except WebSocketDisconnect:
            pass
        finally:
            receive.cancel()
            if pending is not None:
                pending.cancel()
            self.store.clients.discard(queue)


class SocialServiceFactory:
    @staticmethod
    def create(provider: str) -> FastAPI:
        if provider not in {"twitter", "truthsocial"}:
            raise ValueError("unsupported social provider")
        return SocialService(provider).app
