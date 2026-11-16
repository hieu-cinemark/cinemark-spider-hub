import asyncio

import pytest

from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE
from social_crawler.consumer import common
from social_crawler.consumer.jobs import search
from social_crawler.runtime import pool


@pytest.fixture(autouse=True)
def _combine_on(monkeypatch):
    monkeypatch.setattr(search, "get_setting", lambda key: True)


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


def _run(monkeypatch, codes):
    calls = []

    async def fake_subprocess(args, run_id=None, platform=None):
        calls.append(args[2])
        return codes[len(calls) - 1]

    monkeypatch.setattr(search, "_run_subprocess", fake_subprocess)
    runs = search._tiktok_runs("#BoSuaBay", {})
    asyncio.run(search._run_tiktok_variants(runs, run_id="r", keyword="#BoSuaBay"))
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
