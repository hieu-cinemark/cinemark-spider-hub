"""Lịch crawl Facebook theo hai phần hạn mức (27/10/2026): search_budget (phần fresh/deep, trần theo giờ),
deep_cursor (đào bài cũ lùi dần), tham số job theo lane và giới hạn trong client."""

import time
from datetime import date, timedelta

import pytest

from social_crawler.consumer import loop
from social_crawler.consumer.common import CrawlJobSkipped
from social_crawler.consumer.jobs import search as search_job
from social_crawler.services import deep_cursor, search_budget
from social_crawler.spiders.comet_graphql_client import CometGraphQLClient, RequestBudgetExceeded


class FakeCache:
    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)

    def incr(self, key):
        self.data[key] = int(self.data.get(key) or 0) + 1
        return self.data[key]

    def expire(self, key, ttl):
        pass


TODAY = date(2026, 10, 27)


# --- search_budget --------------------------------------------------------------------------------


def test_shares_split_the_daily_cap(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 500)
    monkeypatch.setattr(search_budget, "FRESH_SHARE", 0.4)
    assert search_budget.share_cap("fresh") == 200
    assert search_budget.share_cap("deep") == 300


def test_count_request_tracks_total_share_and_hour(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 500)
    monkeypatch.setattr(search_budget, "HOURLY_CAP", 60)
    cache = FakeCache()
    for _ in range(3):
        search_budget.count_request("a@x", cache, "deep")
    search_budget.count_request("a@x", cache)
    assert search_budget.used_today("a@x", cache) == 4
    assert search_budget.used_share("a@x", "deep", cache) == 3
    assert search_budget.used_share("a@x", "fresh", cache) == 0
    assert search_budget.used_this_hour("facebook", "a@x", cache) == 4


def test_share_available_does_not_rest_the_account(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 10)
    monkeypatch.setattr(search_budget, "FRESH_SHARE", 0.4)
    cache = FakeCache()
    for _ in range(4):
        search_budget.count_request("a@x", cache, "fresh")
    # Hết phần fresh (4/10) nhưng phần deep vẫn còn - tài khoản vẫn làm được việc đào.
    assert not search_budget.share_available("fresh", cache, "a@x")
    assert search_budget.share_available("deep", cache, "a@x")
    assert search_budget.share_available(None, cache, "a@x")


def test_hour_cap_reached(monkeypatch):
    monkeypatch.setattr(search_budget, "HOURLY_CAP", 2)
    cache = FakeCache()
    assert not search_budget.hour_cap_reached("facebook", "a@x", cache)
    search_budget.count_request("a@x", cache)
    search_budget.count_request("a@x", cache)
    assert search_budget.hour_cap_reached("facebook", "a@x", cache)
    assert not search_budget.hour_cap_reached("facebook", None, cache)


# --- deep_cursor ----------------------------------------------------------------------------------


def _no_resweep(monkeypatch):
    monkeypatch.setattr(deep_cursor, "resweep_due", lambda state, keyword_id, today: False)


def test_backfill_starts_after_the_recent_resweep_range(monkeypatch):
    _no_resweep(monkeypatch)
    job = deep_cursor.next_job("kw_1", "2026-10-23", TODAY, FakeCache())
    assert job["kind"] == "backfill"
    assert job["end"] == TODAY - timedelta(days=deep_cursor.RESWEEP_DAYS)
    assert (job["end"] - job["start"]).days + 1 == deep_cursor.WINDOW_DAYS * deep_cursor.WINDOWS_PER_JOB


def test_backfill_walks_back_and_never_repeats_a_window(monkeypatch):
    _no_resweep(monkeypatch)
    cache = FakeCache()
    first = deep_cursor.next_job("kw_1", "2026-10-23", TODAY, cache)
    deep_cursor.window_done("kw_1", first["start"], "2026-10-23", matched=5, cache=cache)
    second = deep_cursor.next_job("kw_1", "2026-10-23", TODAY, cache)
    assert second["end"] == first["start"] - timedelta(days=1)


def test_done_after_consecutive_empty_windows(monkeypatch):
    _no_resweep(monkeypatch)
    cache = FakeCache()
    start = TODAY - timedelta(days=30)
    for i in range(deep_cursor.EMPTY_WINDOWS_DONE):
        state = deep_cursor.window_done("kw_1", start - timedelta(days=7 * i), "2026-10-23", matched=0, cache=cache)
    assert state["done"]
    assert deep_cursor.next_job("kw_1", "2026-10-23", TODAY, cache) is None


def test_matched_window_resets_the_empty_streak(monkeypatch):
    cache = FakeCache()
    deep_cursor.window_done("kw_1", TODAY - timedelta(days=30), "2026-10-23", matched=0, cache=cache)
    state = deep_cursor.window_done("kw_1", TODAY - timedelta(days=37), "2026-10-23", matched=2, cache=cache)
    assert state["empty"] == 0 and not state["done"]


def test_done_at_the_floor(monkeypatch):
    _no_resweep(monkeypatch)
    cache = FakeCache()
    floor = deep_cursor.floor_for("2026-10-23", TODAY)
    assert floor == date(2026, 10, 23) - timedelta(days=deep_cursor.FLOOR_DAYS)
    state = deep_cursor.window_done("kw_1", floor, "2026-10-23", matched=9, cache=cache)
    assert state["done"]


def test_backfill_job_is_clamped_to_the_floor(monkeypatch):
    _no_resweep(monkeypatch)
    cache = FakeCache()
    floor = deep_cursor.floor_for("2026-10-23", TODAY)
    cache.set("fb_deep:kw_1", {"oldest": (floor + timedelta(days=3)).isoformat()})
    job = deep_cursor.next_job("kw_1", "2026-10-23", TODAY, cache)
    assert job["start"] == floor


def test_resweep_due_weekly_even_when_done():
    cache = FakeCache()
    cache.set("fb_deep:kw_1", {"done": True, "resweep": (TODAY - timedelta(days=7)).isoformat()})
    job = deep_cursor.next_job("kw_1", "2026-10-23", TODAY, cache)
    assert job == {"kind": "resweep", "start": TODAY - timedelta(days=deep_cursor.RESWEEP_DAYS - 1), "end": TODAY}
    deep_cursor.resweep_done("kw_1", TODAY, cache)
    assert deep_cursor.next_job("kw_1", "2026-10-23", TODAY, cache) is None


def test_first_resweep_is_spread_over_the_week():
    due_days = [
        deep_cursor.resweep_due({}, "kw_1", TODAY + timedelta(days=offset))
        for offset in range(deep_cursor.RESWEEP_EVERY_DAYS)
    ]
    assert due_days.count(True) == 1


# --- tham số job theo lane ------------------------------------------------------------------------


def _arg(args, name):
    values = [args[i + 1].split("=", 1)[1] for i in range(len(args) - 1) if args[i + 1].startswith(f"{name}=")]
    return values[-1] if values else None


def test_fresh_lane_args():
    args = search_job._facebook_lane_args({"lane": "fresh", "keyword_id": "kw_1"})
    assert _arg(args, "budget_share") == "fresh"
    assert _arg(args, "recent_post") == "true"
    assert _arg(args, "sweep_days") == "0"
    assert _arg(args, "videos_pages") == "0"


def test_deep_lane_args_follow_the_cursor(monkeypatch):
    job = {"kind": "backfill", "start": date(2026, 9, 1), "end": date(2026, 9, 14)}
    monkeypatch.setattr(deep_cursor, "next_job", lambda keyword_id, released_at: job)
    args = search_job._facebook_lane_args({"lane": "deep", "keyword_id": "kw_1", "released_at": "2026-10-23"})
    assert _arg(args, "budget_share") == "deep"
    assert _arg(args, "deep_kind") == "backfill"
    assert (_arg(args, "start_date"), _arg(args, "end_date")) == ("2026-09-01", "2026-09-14")
    assert _arg(args, "sweep_days") == "14"
    assert _arg(args, "sweep_window_days") == str(deep_cursor.WINDOW_DAYS)
    assert _arg(args, "released_at") == "2026-10-23"
    assert _arg(args, "videos_pages") == "0"


def test_resweep_keeps_the_videos_tab(monkeypatch):
    job = {"kind": "resweep", "start": date(2026, 10, 14), "end": date(2026, 10, 27)}
    monkeypatch.setattr(deep_cursor, "next_job", lambda keyword_id, released_at: job)
    args = search_job._facebook_lane_args({"lane": "deep", "keyword_id": "kw_1"})
    assert _arg(args, "deep_kind") == "resweep"
    assert _arg(args, "videos_pages") is None


def test_deep_job_skipped_when_keyword_exhausted(monkeypatch):
    monkeypatch.setattr(deep_cursor, "next_job", lambda keyword_id, released_at: None)
    with pytest.raises(CrawlJobSkipped):
        search_job._facebook_lane_args({"lane": "deep", "keyword_id": "kw_1"})


def test_stale_threshold_depends_on_lane(monkeypatch):
    now_ms = time.time() * 1000
    two_and_half_hours_ago = now_ms - 2.5 * 3_600_000
    assert loop._stale_facebook_job("facebook", {"lane": "deep"}, two_and_half_hours_ago)
    assert not loop._stale_facebook_job("facebook", {"lane": "fresh"}, two_and_half_hours_ago)
    assert not loop._stale_facebook_job("facebook", {}, two_and_half_hours_ago)


# --- giới hạn trong client ------------------------------------------------------------------------


def _client(platform: str, cache: FakeCache) -> CometGraphQLClient:
    client = CometGraphQLClient.__new__(CometGraphQLClient)
    client.PLATFORM = platform
    client._account = "a@x"
    client._redis = cache
    return client


def test_client_stops_when_its_share_is_used_up(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 10)
    monkeypatch.setattr(search_budget, "FRESH_SHARE", 0.4)
    monkeypatch.setattr(search_budget, "HOURLY_CAP", 0)
    client = _client("facebook", FakeCache())
    client.budget_share = "fresh"
    with pytest.raises(RequestBudgetExceeded):
        for _ in range(10):
            client._count_request()
    # share_hard_cap(fresh) = int(4 * 1.2) = 4 -> request thứ 5 bị chặn
    assert search_budget.used_share("a@x", "fresh", client._redis) == 5


def test_client_stops_at_the_hourly_cap(monkeypatch):
    monkeypatch.setattr(search_budget, "DAILY_CAP", 500)
    monkeypatch.setattr(search_budget, "HOURLY_CAP", 5)
    client = _client("facebook", FakeCache())
    with pytest.raises(RequestBudgetExceeded):
        for _ in range(10):
            client._count_request()
    assert search_budget.used_this_hour("facebook", "a@x", client._redis) == 7


def test_threads_counts_hourly_but_only_enforces_when_asked(monkeypatch):
    monkeypatch.setattr(search_budget, "THREADS_HOURLY_CAP", 2)
    client = _client("threads", FakeCache())
    for _ in range(5):
        client._count_request()  # health check / profiles: chỉ đếm
    assert search_budget.used_this_hour("threads", "a@x", client._redis) == 5
    client.enforce_hourly_cap = True
    with pytest.raises(RequestBudgetExceeded):
        client._count_request()


def test_checkpoint_payload_is_not_an_empty_page():
    """27/10/2026: tài khoản bị khoá trả HTTP 200 + body này - trước đó bị coi là trang 0 kết quả."""
    from social_crawler.spiders.comet_graphql_client import _is_checkpoint_response

    body = {
        "error": 1357053,
        "errorSummary": "Veuillez examiner votre compte",
        "redirectTo": "/checkpoint/1501092823525282/",
    }
    assert _is_checkpoint_response(body)
    assert _is_checkpoint_response({"redirectTo": "/checkpoint/42/"})
    assert _is_checkpoint_response({"message": "checkpoint_required", "status": "fail"})
    assert not _is_checkpoint_response({"error": 1357001})
    assert not _is_checkpoint_response({"data": {"serpResponse": {}}})
