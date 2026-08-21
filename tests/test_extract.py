"""
Regression tests for social_crawler.spiders.facebook.features.search.extract,
run against a real captured Facebook search response (tests/fixtures/facebook_search_response.json).

Facebook's response schema drifts across deploys - if these start failing
with fields silently turning None/empty instead of an assertion mismatch,
that's the signal to recapture the fixture and re-check the field paths in
extract.py against it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from social_crawler.spiders.facebook.features.search.extract import extract_response

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "facebook_search_response.json"


@pytest.fixture(scope="module")
def response() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def extracted(response: dict) -> tuple[list[dict], list[dict]]:
    return extract_response(response)


def test_extracts_at_least_one_post(extracted):
    posts, _ = extracted
    assert len(posts) > 0


def test_post_fields_are_populated(extracted):
    """Guards the Feedback dedup fix: a bare {__typename, id} stub next to
    the fully-populated Feedback node used to silently win the id collision
    and leave comments_count/reactions as None."""
    posts, _ = extracted
    post = next(p for p in posts if p["post_id"] == "122197805540842674")

    assert "Hộ Linh Tráng Sĩ" in post["message"]
    assert post["author_name"] == "Phim Hộ Linh Tráng Sĩ - Bí ẩn mộ Vua Đinh"
    assert post["author_id"] == "61575280221391"
    assert post["timestamp"] == 1787038448

    assert isinstance(post["comments_count"], int) and post["comments_count"] > 0
    assert isinstance(post["shares_count"], int) and post["shares_count"] > 0

    assert post["reactions"]
    assert post["reactions_count"] == sum(post["reactions"].values())
    assert post["reactions"]["Like"] > 0


def test_hashtags_extracted_from_message(extracted):
    posts, _ = extracted
    post = next(p for p in posts if p["post_id"] == "122197805540842674")
    assert post["hashtags"] == ["holinhtrangsi", "bhd", "tv360", "nsndtulong"]


def test_video_media_fields(extracted):
    posts, _ = extracted
    post = next(p for p in posts if p["post_id"] == "122197805540842674")
    assert post["media_type"] == "Video"
    assert post["media_url"] == "https://www.facebook.com/reel/2218251595688626/"
    assert post["duration_seconds"] == 11.7


def test_no_empty_entities_leak_through(extracted):
    """Guards the noise filter: bare references with neither a name nor a
    url (e.g. {__typename, id}) should never end up in the output."""
    _, others = extracted
    assert others
    assert all(entity["name"] or entity["url"] for entity in others)


def test_entities_are_deduplicated(extracted):
    _, others = extracted
    ids = [entity["id"] for entity in others]
    assert len(ids) == len(set(ids))
