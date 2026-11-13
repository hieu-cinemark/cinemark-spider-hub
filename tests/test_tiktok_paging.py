"""TikTok: thử lại lần gọi có ký đầu tiên, cỡ trang comment, đổi danh tính khi lật trang video kênh.

Không gọi mạng: client và response đều giả lập."""

from __future__ import annotations

import asyncio
from unittest.mock import Mock

import pytest

from social_crawler.spiders.tiktok import client as tc
from social_crawler.spiders.tiktok.features.channel_videos import search as channel
from social_crawler.spiders.tiktok.features.comments import comments


def _bare_client() -> tc.TikTokClient:
    c = object.__new__(tc.TikTokClient)
    c._answered = False
    c._synthetic = True
    return c


def test_first_signed_call_is_retried_once_on_the_same_session():
    c = _bare_client()
    c._request_once = Mock(side_effect=[tc.TikTokBlockedError("empty"), {"ok": 1}])

    assert c._request("u", {}, "r") == {"ok": 1}
    assert c._request_once.call_count == 2
    # Lần rỗng đầu không tính vào chuỗi tự tắt tài khoản.
    assert c._request_once.call_args_list[0].kwargs == {"count_block": False}
    assert c._answered is True


def test_after_an_answer_an_empty_body_is_not_retried():
    c = _bare_client()
    c._answered = True
    c._request_once = Mock(side_effect=tc.TikTokBlockedError("empty"))

    with pytest.raises(tc.TikTokBlockedError):
        c._request("u", {}, "r")
    assert c._request_once.call_count == 1


def test_two_empty_first_calls_still_raise():
    c = _bare_client()
    c._request_once = Mock(side_effect=tc.TikTokBlockedError("empty"))

    with pytest.raises(tc.TikTokBlockedError):
        c._request("u", {}, "r")
    assert c._request_once.call_count == 2
    assert c._answered is False


def test_comment_pages_are_50_and_replies_stay_20():
    assert comments.DEFAULT_COUNT == 50
    assert comments.REPLY_COUNT == 20


class _FakeChannelClient:
    created = 0

    def __init__(self, *args, **kwargs):
        type(self).created += 1
        self.calls = 0

    def warm_session(self):
        pass

    def resolve_sec_uid(self, username):
        return "MS4wLjABfake"

    def list_posts(self, sec_uid, username, *, cursor=0, count=35):
        self.calls += 1
        # Một phiên trả lời quá ngưỡng thì coi như bị TikTok cắt.
        if self.calls > channel.POSTS_CALLS_PER_IDENTITY:
            raise tc.TikTokBlockedError("exit IP spent")
        page = cursor + 1
        return {"itemList": [{"id": f"v{page}"}], "hasMore": True, "cursor": page}


def test_channel_rotates_identity_before_tiktok_cuts_the_exit(monkeypatch):
    _FakeChannelClient.created = 0
    monkeypatch.setattr(channel, "TikTokChannelClient", _FakeChannelClient)
    monkeypatch.setattr(channel, "extract_response", lambda r: [{"video_id": i["id"]} for i in r["itemList"]])
    monkeypatch.setattr(channel, "TikTokChannelVideoItem", lambda **kw: kw)
    spider = channel.TikTokChannelVideosSpider(username="someone", max_pages=30)

    async def collect():
        return [item async for item in spider._crawl_with_fresh_identity()]

    items = asyncio.run(collect())
    assert len(items) == 30
    assert _FakeChannelClient.created == 3  # trang 1-12, 13-24, 25-30
