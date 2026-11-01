import hashlib
import json

from common.news.fixtures import TimelineFixtures


def verify(context):
    root = context.root / "config"
    provenance = json.loads((root / "twitter/provenance.json").read_text())
    actual = hashlib.sha256((root / "twitter/openapi.json").read_bytes()).hexdigest()
    assert actual == provenance["sha256"], "X official schema hash mismatch"
    assert provenance["official"] is True
    truth = json.loads((root / "truthsocial/provenance.json").read_text())
    assert truth["official_openapi_available"] is False, "do not misrepresent the local compatibility contract"
    for provider in ("twitter", "truthsocial"):
        fixtures = TimelineFixtures(provider, root)
        posts = fixtures.create()
        fixtures.response(posts)
    return {"x_schema_version": provenance["version"], "x_sha256": actual,
            "truthsocial": "Mastodon-compatible local subset; official licensed schema unavailable",
            "fixtures": "synthetic posts validated; no claim they are real public posts"}
