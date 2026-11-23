"""Thăm dò quá chỗ TikTok báo hết feed hashtag (17/11/2026).

hasMore=false chỉ là hết của MỘT cursor: gửi cursor lớn hơn vẫn ra video (xem
social_crawler/spiders/tiktok/features/hashtag_search/probe.py). Không gọi mạng.
"""

from __future__ import annotations

import asyncio
from collections import Counter

from social_crawler.constants.tiktok import PROBE_CURSOR_STEPS
from social_crawler.spiders.tiktok.client import TikTokBlockedError
from social_crawler.spiders.tiktok.features.hashtag_search import search
from social_crawler.spiders.tiktok.features.hashtag_search.probe import ProbeBudget, probe_past_end


def _page(*video_ids: str) -> dict:
    return {"itemList": [{"id": vid, "author": {}, "stats": {}} for vid in video_ids]}


def _run(fetch, base: int, budget: ProbeBudget):
    return asyncio.run(probe_past_end(fetch, base, budget, hashtag="phim"))


def test_probe_returns_the_first_cursor_that_still_has_videos():
    sent: list[int] = []

    async def fetch(cursor: int) -> dict:
        sent.append(cursor)
        return _page("v1") if cursor == 292 + PROBE_CURSOR_STEPS[1] else _page()

    budget = ProbeBudget(10)
    found = _run(fetch, 292, budget)

    assert found is not None
    cursor, response = found
    assert cursor == 292 + PROBE_CURSOR_STEPS[1]
    assert response["itemList"][0]["id"] == "v1"
    assert sent == [292 + PROBE_CURSOR_STEPS[0], cursor]  # dừng ngay khi có video
    assert budget.left == 8


def test_probe_gives_up_when_every_step_is_empty():
    async def fetch(_cursor: int) -> dict:
        return _page()

    budget = ProbeBudget(99)
    assert _run(fetch, 10, budget) is None
    assert budget.left == 99 - len(PROBE_CURSOR_STEPS)


def test_an_empty_body_is_not_the_end_of_the_probe():
    """Body rỗng ở vùng rìa là chuyện thường - thử bước nhảy kế, không coi là danh tính hỏng."""
    calls: list[int] = []

    async def fetch(cursor: int) -> dict:
        calls.append(cursor)
        if len(calls) == 1:
            raise TikTokBlockedError("empty body")
        return _page("v2")

    found = _run(fetch, 0, ProbeBudget(5))

    assert found is not None and found[0] == PROBE_CURSOR_STEPS[1]
    assert len(calls) == 2


def test_budget_is_spent_across_the_whole_run():
    async def fetch(_cursor: int) -> dict:
        return _page()

    budget = ProbeBudget(2)
    assert _run(fetch, 0, budget) is None
    assert budget.left == 0
    # Hết ngân sách: lần thăm dò sau không gửi request nào nữa.
    sent: list[int] = []

    async def counted(cursor: int) -> dict:
        sent.append(cursor)
        return _page("v3")

    assert _run(counted, 100, budget) is None
    assert sent == []


def test_probe_is_off_when_the_budget_is_zero():
    async def fetch(_cursor: int) -> dict:  # pragma: no cover - không được gọi
        raise AssertionError("tiktok_probe_requests=0 thì không được gửi request nào")

    assert _run(fetch, 0, ProbeBudget(0)) is None


def test_probe_requests_do_not_count_toward_the_account_block_streak():
    """3 lần body rỗng vốn tự tắt tài khoản TikTok (client.py) - thăm dò phải gọi với count_block=False."""
    from unittest.mock import Mock

    from social_crawler.spiders.tiktok import hashtag_client

    c = object.__new__(hashtag_client.TikTokHashtagClient)
    c._request = Mock(return_value={"itemList": []})

    c.search_hashtag("123", 300, 30, "phim", count_block=False)

    assert c._request.call_args.kwargs["count_block"] is False
    assert c._request.call_args.args[1]["cursor"] == "300"


class _Kafka:
    async def publish(self, **_kwargs) -> bool:
        return True


def _spider(max_pages: int = 50):
    """Spider đủ dùng cho _crawl: không Scrapy, không Redis, không Kafka thật."""
    spider = object.__new__(search.TikTokHashtagSearchSpider)
    spider.hashtag = "phim"
    spider.keyword_id = None
    spider.count = 30
    spider.max_pages = max_pages
    spider._cache = None
    spider._kafka = _Kafka()
    spider._page_new_posts = 0
    spider._post_count = 0
    spider._related_hashtag_counts = Counter()
    return spider


def _drive(spider, pages: dict[int, dict], monkeypatch, budget: int = 30) -> list[int]:
    """Chạy _crawl với một client giả; trả về các cursor đã gửi. `pages` thiếu cursor nào thì trang đó rỗng."""
    sent: list[int] = []
    monkeypatch.setattr(search, "get_setting", lambda name: budget if name == "tiktok_probe_requests" else 0)

    class Client:
        def search_hashtag(self, _challenge_id, cursor, _count, _hashtag, count_block=True):
            sent.append(cursor)
            return pages.get(cursor, _page())

    async def run():
        async for _item in spider._crawl(Client(), "cid"):
            pass

    asyncio.run(run())
    return sent


def test_a_feed_that_ends_on_page_one_is_not_probed(monkeypatch):
    """Hashtag bé hết thật (đo #meminbymoli: 17 video) - 5 request thăm dò ra 0 video, nên không thăm dò."""
    first = {**_page("a", "b"), "hasMore": False, "cursor": 17}

    sent = _drive(_spider(), {0: first}, monkeypatch)

    assert sent == [0]


def test_the_probe_starts_from_the_cursor_that_was_sent_not_the_one_tiktok_returned(monkeypatch):
    """Đo 17/11: feed hết ở cursor đã gửi 90 trong khi response ghi 110, và video còn lại nằm ở 91-100."""
    pages = {
        0: {**_page("a"), "hasMore": True, "cursor": 30},
        30: {**_page("b"), "hasMore": True, "cursor": 60},
        60: {**_page("c"), "hasMore": True, "cursor": 90},
        90: {**_page(), "hasMore": False, "cursor": 110},
        91: {**_page("d"), "hasMore": False, "cursor": 93},
    }

    sent = _drive(_spider(), pages, monkeypatch)

    assert sent[:4] == [0, 30, 60, 90]
    assert sent[4] == 91  # 90 + bước nhảy đầu, không phải 110
    assert 110 not in sent


def test_the_probe_budget_bounds_the_extra_requests(monkeypatch):
    pages = {
        0: {**_page("a"), "hasMore": True, "cursor": 30},
        30: {**_page("b"), "hasMore": False, "cursor": 60},
    }

    sent = _drive(_spider(), pages, monkeypatch, budget=2)

    assert sent == [0, 30, 31, 38]  # hết ngân sách sau 2 lần thăm dò


def test_the_probe_is_off_when_the_setting_is_zero(monkeypatch):
    pages = {
        0: {**_page("a"), "hasMore": True, "cursor": 30},
        30: {**_page("b"), "hasMore": False, "cursor": 60},
    }

    assert _drive(_spider(), pages, monkeypatch, budget=0) == [0, 30]


def test_a_probe_page_of_only_duplicates_does_not_count_as_still_having_data(monkeypatch):
    """Vùng rìa trả rất nhiều bài trùng (đo #traigiamhanhphucbymoli: 8 request, 77 bài trùng, 2 bài mới)."""
    seen = {"a", "b"}
    sent: list[int] = []

    async def fetch(cursor: int) -> dict:
        sent.append(cursor)
        return _page("a", "b") if cursor < 60 else _page("c")

    found = asyncio.run(
        probe_past_end(
            fetch,
            0,
            ProbeBudget(10),
            hashtag="phim",
            has_new=lambda videos: any(v["video_id"] not in seen for v in videos),
        )
    )

    assert found is not None and found[0] == 60  # bỏ qua các trang toàn bài trùng
    assert sent == [1, 8, 15, 30, 60]
