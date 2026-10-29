"""Deterministic external OpenAI Chat Completions boundary, never a trading engine."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import time

from fastapi import FastAPI, HTTPException, Request


class OpenAIService:
    def __init__(self) -> None:
        self.token = os.environ.get("MARKETPULSE_MOCK_TOKEN", "")
        if not self.token:
            raise ValueError("MARKETPULSE_MOCK_TOKEN is required")
        self.calls: list[dict] = []
        self.total_calls = 0
        self.app = FastAPI(title="Synthetic OpenAI boundary")
        self.app.add_api_route("/healthz", self.health)
        self.app.add_api_route("/stats", self.stats)
        self.app.add_api_route("/v1/chat/completions", self.complete, methods=["POST"])

    async def health(self) -> dict:
        return {"status": "ok", "synthetic": True}

    def authorize(self, request: Request) -> None:
        if not hmac.compare_digest(request.headers.get("authorization", ""), f"Bearer {self.token}"):
            raise HTTPException(401, "unauthorized")

    async def stats(self, request: Request) -> dict:
        self.authorize(request)
        return {"requests": self.total_calls, "calls": self.calls}

    async def complete(self, request: Request) -> dict:
        self.authorize(request)
        body = await request.json()
        if body.get("stream"):
            raise HTTPException(400, "fixture only supports bounded non-streaming completions")
        if not body.get("messages") or not body.get("model"):
            raise HTTPException(400, "model and messages are required")
        encoded = json.dumps(body["messages"], sort_keys=True).encode()
        digest = hashlib.sha256(encoded).hexdigest()
        self.calls.append({"input_sha256": digest, "traceparent": request.headers.get("traceparent"),
                           "model": body["model"]})
        self.total_calls += 1
        self.calls = self.calls[-100:]
        citations = [{"source": "input", "input_sha256": digest}]
        try:
            user = next(message for message in reversed(body["messages"]) if message["role"] == "user")
            supplied = json.loads(user["content"])
            if isinstance(supplied, dict) and supplied.get("facts"):
                fact = supplied["facts"][0]
                citations = [{"source": fact["source"], "source_event_id": fact["source_event_id"]}]
        except (KeyError, ValueError, StopIteration, TypeError):
            pass
        evaluation = {
            "version": 1, "model_version": "marketpulse-fixture-v1",
            "probabilities": {"trend": 0.5, "range": 0.2, "stabilizing": 0.15, "suppression": 0.1, "unknown": 0.05},
            "classification": "trend", "parameters": {"target_exposure": "0.10", "max_position_ratio": "0.20"},
            "facts": citations,
            "inferences": ["Synthetic deterministic market classification."],
            "valid_for_seconds": 60,
        }
        return {"id": "chatcmpl-" + digest[:24], "object": "chat.completion", "created": int(time.time()),
                "model": body["model"], "choices": [{"index": 0, "finish_reason": "stop",
                "message": {"role": "assistant", "content": json.dumps(evaluation)}}],
                "usage": {"prompt_tokens": len(encoded) // 4, "completion_tokens": 100,
                          "total_tokens": len(encoded) // 4 + 100}}


class OpenAIServiceFactory:
    @staticmethod
    def create() -> FastAPI:
        return OpenAIService().app
