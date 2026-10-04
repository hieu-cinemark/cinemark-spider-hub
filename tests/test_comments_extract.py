"""
Test hồi quy cho social_crawler.spiders.facebook.features.comments.extract, chạy trên một
response danh sách comment thật bắt được (tests/fixtures/facebook_comments_response.json).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from social_crawler.spiders.facebook.features.comments.extract import extract_comments, find_comments_page_info

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "facebook_comments_response.json"
PAGE2_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "facebook_comments_page2_response.json"


@pytest.fixture(scope="module")
def response() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def comments(response: dict) -> list[dict]:
    return extract_comments(response)


def test_extracts_all_comments(comments):
    assert len(comments) == 10


def test_comment_fields_are_populated(comments):
    comment = next(c for c in comments if c["legacy_comment_id"] == "928856216291273")
    assert comment["comment_id"] == "Y29tbWVudDoxMjIxOTc1Mzk5OTI4NDI2NzRfOTI4ODU2MjE2MjkxMjcz"
    assert comment["message"] == "Cần lắm có ngay cintour trong ninh bình luôn ạ🥰"
    assert comment["date"] == "2026-08-18 12:40:51"
    assert comment["timestamp"] == 1787056851
    assert comment["author_name"] == "Linh Linh"
    assert comment["author_id"] == "61581568353073"
    assert comment["author_url"].startswith("https://www.facebook.com/")
    assert comment["author_gender"] == "FEMALE"
    assert comment["author_profile_picture"].startswith("https://")


def test_reactions_count_extracted(comments):
    """Bảo vệ _find_reactors_count: một comment có like thật phải báo số khác 0, không âm thầm
    quay về None/0 cho mọi comment."""
    comment = next(c for c in comments if c["legacy_comment_id"] == "1566264664963659")
    assert comment["reactions_count"] == 1


def test_no_attachment_leaves_fields_empty(comments):
    comment = next(c for c in comments if c["legacy_comment_id"] == "928856216291273")
    assert comment["is_sticker"] is False
    assert comment["is_gif"] is False
    assert comment["sticker_url"] is None
    assert comment["gif"] is None


def test_sticker_attachment_extracted(comments):
    comment = next(c for c in comments if c["legacy_comment_id"] == "1570884421074112")
    assert comment["is_sticker"] is True
    assert comment["is_gif"] is False
    assert comment["sticker_url"].startswith("https://")


def test_gif_attachment_extracted(comments):
    comment = next(c for c in comments if c["legacy_comment_id"] == "1062941499569301")
    assert comment["is_gif"] is True
    assert comment["is_sticker"] is False
    assert "giphy" in comment["gif"]


def test_page_info_reports_next_page_available(response):
    """Bảo vệ find_comments_page_info: một phép tìm đệ quy ngây thơ nhặt page_info chuỗi reply
    riêng (luôn False) của một comment trước khi tới được cái thật của danh sách comment - bài
    trong fixture này có nhiều comment hơn mức vừa một trang, nên has_next_page phải trả về
    True."""
    page_info = find_comments_page_info(response)
    assert page_info is not None
    assert page_info["has_next_page"] is True
    assert page_info["end_cursor"]


def test_page2_has_no_overlap_with_page1(comments):
    page2_response = json.loads(PAGE2_FIXTURE_PATH.read_text(encoding="utf-8"))
    page2_comments = extract_comments(page2_response)

    assert len(page2_comments) == 3
    page1_ids = {c["comment_id"] for c in comments}
    page2_ids = {c["comment_id"] for c in page2_comments}
    assert page1_ids.isdisjoint(page2_ids)

    page2_info = find_comments_page_info(page2_response)
    assert page2_info["has_next_page"] is False
