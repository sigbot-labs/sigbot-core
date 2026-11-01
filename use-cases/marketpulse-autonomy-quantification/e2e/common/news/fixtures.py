"""Synthetic boundary fixtures validated against a pinned upstream contract."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from jsonschema import Draft202012Validator


class TimelineFixtures:
    def __init__(self, provider: str, config_root: Path, base_time: str | None = None) -> None:
        self.provider = provider
        self.base_time = datetime.fromisoformat((base_time or "2026-10-27T08:00:00Z").replace("Z", "+00:00"))
        if self.base_time.tzinfo is None:
            raise ValueError("fixture base time requires an explicit timezone")
        self.document = json.loads((config_root / provider / "openapi.json").read_text())

    def create(self) -> list[dict]:
        posts = []
        for seq, text in enumerate((
            "Synthetic fixture: policy announcement increases market uncertainty.",
            "Synthetic fixture: liquidity recovers while volatility remains elevated.",
            "Synthetic fixture: funding stabilizes; this is not a real public statement.",
            "Synthetic fixture: traders await a scheduled policy release.",
            "Synthetic fixture: credit conditions remain unchanged.",
            "Synthetic fixture: market depth recovers after a liquidity shock.",
        ), 1):
            created_at = (self.base_time + timedelta(seconds=seq)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            milliseconds = int(datetime.fromisoformat(created_at.replace("Z", "+00:00")).timestamp() * 1000)
            identifier = ((milliseconds - 1288834974657) << 22) + seq if self.provider == "twitter" else (milliseconds << 16) + seq
            post = {
                "id": str(identifier),
                "created_at": created_at,
            }
            if self.provider == "twitter":
                post.update(text=text, author_id="2244994945", lang="en",
                            edit_history_post_ids=[post["id"]])
            else:
                post.update(content=f"<p>{text}</p>", visibility="public", sensitive=False,
                            spoiler_text="", language="en", uri=f"https://truthsocial.com/users/fixture/statuses/{post['id']}",
                            url=f"https://truthsocial.com/@fixture/posts/{post['id']}",
                            account={"id": "100000000000001", "username": "fixture", "acct": "fixture",
                                     "display_name": "Synthetic fixture", "url": "https://truthsocial.com/@fixture"},
                            reblogs_count=0, favourites_count=0, replies_count=0,
                            media_attachments=[], mentions=[], tags=[], emojis=[], reblog=None)
            self.validate_post(post)
            posts.append(post)
        return posts

    def validate_post(self, post: dict) -> None:
        name = "Post" if self.provider == "twitter" else "Status"
        schema = {"$ref": f"#/components/schemas/{name}", "components": self.document["components"]}
        Draft202012Validator(schema).validate(post)
        # X's current schema marks all fields optional. Domain requirements are stricter.
        for key in ("id", "created_at", "text" if self.provider == "twitter" else "content"):
            if not post.get(key):
                raise ValueError(f"missing timeline field: {key}")

    def response(self, posts: list[dict], next_token: str | None = None) -> dict | list:
        if self.provider != "twitter":
            return posts
        meta = {"result_count": len(posts)}
        if posts:
            meta.update(newest_id=max((post["id"] for post in posts), key=int),
                        oldest_id=min((post["id"] for post in posts), key=int))
        if next_token:
            meta["next_token"] = next_token
        response = {"data": posts, "meta": meta}
        schema = {"$ref": "#/components/schemas/GetUsersPostsResponse", "components": self.document["components"]}
        Draft202012Validator(schema).validate(response)
        return response
