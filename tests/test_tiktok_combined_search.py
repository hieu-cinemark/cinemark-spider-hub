import asyncio

import pytest

from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE
from social_crawler.consumer import common
from social_crawler.consumer.jobs import search
from social_crawler.runtime import pool


@pytest.fixture(autouse=True)
def _combine_on(monkeypatch):
    monkeypatch.setattr(search, "get_setting", lambda key: True)
    monkeypatch.setattr(search.search_pause, "is_paused", lambda cache=None: False)


def _spiders(runs):
    return [(label, args[2], args[4]) for label, args in runs]


def test_hashtag_and_text_forms():
    assert search.tiktok_hashtag_from_text("Bò Sữa Bay") == "bosuabay"
    assert search.tiktok_hashtag_from_text("Đất Rừng Phương Nam 2") == "datrungphuongnam2"
    assert search.tiktok_search_text_from_hashtag("#BoSuaBay") == "Bo Sua Bay"
    assert search.tiktok_search_text_from_hashtag("anmangxemhoanhao") == "anmangxemhoanhao"


def test_hashtag_job_also_searches_keyword():
    runs = search._tiktok_runs("#BoSuaBay", {"keyword_id": "k1", "max_pages": 5})
    assert _spiders(runs) == [
        ("hashtag=#BoSuaBay", common.TIKTOK_HASHTAG_SPIDER, "hashtag=BoSuaBay"),
        ("keyword=Bo Sua Bay", common.TIKTOK_KEYWORD_SPIDER, "keyword=Bo Sua Bay"),
    ]
    assert all("keyword_id=k1" in args and "max_pages=5" in args for _, args in runs)


def test_text_job_also_reads_hashtag_feed():
    runs = search._tiktok_runs("Bò Sữa Bay", {})
    assert _spiders(runs) == [
        ("keyword=Bò Sữa Bay", common.TIKTOK_KEYWORD_SPIDER, "keyword=Bò Sữa Bay"),
        ("hashtag=#bosuabay", common.TIKTOK_HASHTAG_SPIDER, "hashtag=bosuabay"),
    ]


def test_bfs_hops_stay_hashtag_only():
    runs = search._tiktok_runs("#ostbosuabay", {"bfs_depth": 1})
    assert [spider for _, spider, _ in _spiders(runs)] == [common.TIKTOK_HASHTAG_SPIDER]


def _run(monkeypatch, codes, keyword="#BoSuaBay"):
    calls = []

    async def fake_subprocess(args, run_id=None, platform=None):
        calls.append(args[2])
        return codes[len(calls) - 1]

    monkeypatch.setattr(search, "_run_subprocess", fake_subprocess)
    runs = search._tiktok_runs(keyword, {})
    asyncio.run(search._run_tiktok_variants(runs, run_id="r", keyword=keyword))
    return calls


def test_one_variant_failing_still_counts_as_done(monkeypatch):
    assert _run(monkeypatch, [1, 0]) == [common.TIKTOK_HASHTAG_SPIDER, common.TIKTOK_KEYWORD_SPIDER]


def test_both_failing_fails_the_job(monkeypatch):
    with pytest.raises(common.CrawlJobFailed):
        _run(monkeypatch, [1, 1])


def test_proxy_exhausted_stops_immediately(monkeypatch):
    with pytest.raises(pool.ProxyPoolExhaustedError):
        _run(monkeypatch, [PROXY_EXHAUSTED_EXIT_CODE, 0])


def test_switch_off_runs_single_variant(monkeypatch):
    monkeypatch.setattr(search, "get_setting", lambda key: False)
    assert [s for _, s, _ in _spiders(search._tiktok_runs("#BoSuaBay", {}))] == [common.TIKTOK_HASHTAG_SPIDER]
    assert [s for _, s, _ in _spiders(search._tiktok_runs("Bò Sữa Bay", {}))] == [common.TIKTOK_KEYWORD_SPIDER]


# --- tìm từ khoá nghỉ khi IP bị phạt (16/11/2026) ---


def test_paused_keyword_variant_does_not_fail_and_hashtag_still_runs(monkeypatch):
    from social_crawler.constants.tiktok import SEARCH_PAUSED_EXIT_CODE

    # Job chữ: từ khoá chạy trước (bị nghỉ, mã 76) rồi tới hashtag (chạy được) -> job xong, không lỗi.
    assert _run(monkeypatch, [SEARCH_PAUSED_EXIT_CODE, 0], keyword="Bò Sữa Bay") == [
        common.TIKTOK_KEYWORD_SPIDER,
        common.TIKTOK_HASHTAG_SPIDER,
    ]


def test_paused_skips_spawning_keyword_spider(monkeypatch):
    monkeypatch.setattr(search.search_pause, "is_paused", lambda cache=None: True)
    assert _run(monkeypatch, [0], keyword="Bò Sữa Bay") == [common.TIKTOK_HASHTAG_SPIDER]


def test_only_variant_paused_is_skip_not_failure(monkeypatch):
    monkeypatch.setattr(search, "get_setting", lambda key: False)  # không gộp: chỉ còn lượt từ khoá
    monkeypatch.setattr(search.search_pause, "is_paused", lambda cache=None: True)
    with pytest.raises(common.TikTokSearchPaused):
        _run(monkeypatch, [], keyword="Bò Sữa Bay")
