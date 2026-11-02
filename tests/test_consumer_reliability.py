"""consumer/loop + dispatch (review 02/11/2026): job chỉ được coi là xong khi đã có kết cục - huỷ giữa chừng, xếp lại hỏng, job
hết hạn và message cũ hơn lần Dừng đều không được mất dấu."""

from __future__ import annotations

import asyncio
import json
import time
from types import SimpleNamespace
from typing import ClassVar

import pytest

from social_crawler.consumer import common, dispatch, loop, process
from social_crawler.runtime import pool, task_queue
from tests.test_priority_lane import FakeCache, _setup


def _cancelling_handler():
    async def handler(request):
        raise asyncio.CancelledError

    return handler


def test_cancelled_priority_job_stays_in_processing(monkeypatch):
    _setup(monkeypatch, _cancelling_handler())
    FakeCache.lists["crawl_priority:facebook"] = [json.dumps({"run_id": "a"})]
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(loop._drain_priority_queue("facebook"))
    assert [json.loads(r) for r in FakeCache.lists["crawl_priority:facebook:processing"]] == [{"run_id": "a"}]
    loop._recover_priority_queue("facebook")
    assert [json.loads(r) for r in FakeCache.lists["crawl_priority:facebook"]] == [{"run_id": "a"}]


def test_cancelled_idle_job_stays_in_processing(monkeypatch):
    _setup(monkeypatch, _cancelling_handler())
    FakeCache().lpush_json("crawl_idle:facebook", {"type": "post_metrics", "run_id": "a"})
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(loop._run_one_idle_job("facebook"))
    assert len(FakeCache.lists["crawl_idle:facebook:processing"]) == 1


def test_retryable_outcome_is_requeued_on_priority_lane(monkeypatch):
    async def handler(request):
        raise common.CrawlJobRetryable("publish failed")

    _setup(monkeypatch, handler)
    FakeCache.lists["crawl_priority:facebook"] = [json.dumps({"run_id": "a"})]
    asyncio.run(loop._drain_priority_queue("facebook"))
    assert [json.loads(r)["_proxy_exhausted_retries"] for r in FakeCache.lists["crawl_priority:facebook"]] == [1]


class _Publisher:
    def __init__(self, ok, started=True):
        self.ok = ok
        self.started = started
        self.sent = []
        self.start_calls = 0

    async def start(self):
        self.start_calls += 1
        self.started = True

    async def stop(self):
        pass

    async def publish(self, topic, key, value):
        self.sent.append(value)
        return self.ok


def _requeue(publisher, request):
    return asyncio.run(loop._requeue_after_proxy_exhaustion(publisher, request, platform="facebook"))


def test_requeue_reports_failed_publish(monkeypatch):
    monkeypatch.setattr(loop, "get_proxy_settings", lambda: {"exhausted_max_requeues": 3})
    assert _requeue(_Publisher(ok=False), {"run_id": "a"}) is False
    assert _requeue(_Publisher(ok=True), {"run_id": "a"}) is True


def test_requeue_restarts_dead_publisher(monkeypatch):
    monkeypatch.setattr(loop, "get_proxy_settings", lambda: {"exhausted_max_requeues": 3})
    publisher = _Publisher(ok=True, started=False)
    assert _requeue(publisher, {"run_id": "a"}) is True
    assert publisher.start_calls == 1


def test_requeue_gave_up_lets_message_commit(monkeypatch):
    monkeypatch.setattr(loop, "get_proxy_settings", lambda: {"exhausted_max_requeues": 1})
    publisher = _Publisher(ok=False)
    assert _requeue(publisher, {"run_id": "a", "_proxy_exhausted_retries": 1}) is True
    assert publisher.sent == []


class _Stop(BaseException):
    """Thoát vòng while True của _run_platform_consumer_once sau message cần kiểm tra."""


class _Consumer:
    instance = None

    def __init__(self, *args, **kwargs):
        self.messages = list(_Consumer.feed)
        self.commits = 0
        self.seeks = []
        _Consumer.instance = self

    feed: ClassVar[list] = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def commit(self):
        self.commits += 1

    def seek(self, tp, offset):
        self.seeks.append((tp.topic, tp.partition, offset))

    def assignment(self):
        return set()

    async def getmany(self, timeout_ms=0, max_records=None):
        if not self.messages:
            raise _Stop
        return {"tp": [self.messages.pop(0)]}


def _loop_setup(monkeypatch, handler, *, publish_ok=True):
    _Consumer.feed = []
    publisher = _Publisher(ok=publish_ok)
    monkeypatch.setattr(loop, "AIOKafkaConsumer", _Consumer)
    monkeypatch.setattr(loop, "KafkaPublisher", lambda: publisher)
    monkeypatch.setattr(loop, "_handle_request", handler)
    monkeypatch.setattr(loop, "_clear_leftover_job_key", lambda platform: None)
    monkeypatch.setattr(loop, "_recover_priority_queue", lambda platform: None)
    monkeypatch.setattr(loop, "_recover_idle_queue", lambda platform: None)
    monkeypatch.setattr(loop, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(
        loop,
        "get_proxy_settings",
        lambda: {
            "exhausted_max_requeues": 3,
            "exhausted_backoff_base_seconds": 0,
            "exhausted_backoff_growth_factor": 1,
            "exhausted_backoff_max_seconds": 0,
        },
    )

    async def nothing(*args, **kwargs):
        return None

    async def no_pause(*args, **kwargs):
        return False

    for name in ("_wait_out_quiet_hours", "_yield_partitions_while_no_account", "_wait_out_hourly_cap"):
        monkeypatch.setattr(loop, name, nothing)
    monkeypatch.setattr(loop, "_drain_priority_queue", nothing)
    monkeypatch.setattr(loop, "_sleep_interruptible", no_pause)
    return publisher


def _message(request, *, offset=7, timestamp=None):
    return SimpleNamespace(
        value=request, topic="crawl_requests", partition=1, offset=offset, timestamp=timestamp or time.time() * 1000
    )


def _run_once():
    with pytest.raises(_Stop):
        asyncio.run(loop._run_platform_consumer_once("facebook", "group"))
    return _Consumer.instance


def test_failed_requeue_keeps_offset_and_rereads_message(monkeypatch):
    async def handler(request):
        raise pool.ProxyPoolExhaustedError("no proxy")

    publisher = _loop_setup(monkeypatch, handler, publish_ok=False)
    _Consumer.feed = [_message({"platform": "facebook", "run_id": "a"})]
    consumer = _run_once()
    assert len(publisher.sent) == 1
    assert consumer.commits == 0
    assert consumer.seeks == [("crawl_requests", 1, 7)]


def test_successful_requeue_commits(monkeypatch):
    async def handler(request):
        raise common.CrawlJobRetryable("publish failed")

    _loop_setup(monkeypatch, handler)
    _Consumer.feed = [_message({"platform": "facebook", "run_id": "a"})]
    consumer = _run_once()
    assert consumer.commits == 1 and consumer.seeks == []


def test_stale_job_is_finished_before_commit(monkeypatch):
    async def handler(request):
        raise AssertionError("job hết hạn không được chạy")

    _loop_setup(monkeypatch, handler)
    finished = []
    monkeypatch.setattr(loop, "finish_task", lambda request, status, error=None: finished.append((status, error)))
    _Consumer.feed = [_message({"platform": "facebook", "run_id": "a"}, timestamp=(time.time() - 7 * 3600) * 1000)]
    consumer = _run_once()
    assert finished == [("skipped", "stale")]
    assert consumer.commits == 1


def test_stale_job_redis_failure_does_not_kill_loop(monkeypatch):
    async def handler(request):
        raise AssertionError("job hết hạn không được chạy")

    _loop_setup(monkeypatch, handler)

    def boom(request, status, error=None):
        raise RuntimeError("redis down")

    monkeypatch.setattr(loop, "finish_task", boom)
    _Consumer.feed = [_message({"platform": "facebook", "run_id": "a"}, timestamp=(time.time() - 7 * 3600) * 1000)]
    assert _run_once().commits == 1


def test_kafka_timestamp_fills_published_at(monkeypatch):
    seen = []

    async def handler(request):
        seen.append(request)
        return True

    _loop_setup(monkeypatch, handler)
    _Consumer.feed = [
        _message({"platform": "facebook", "run_id": "a"}, timestamp=1_000_000),
        _message({"platform": "facebook", "run_id": "b", "published_at": 5.0}, timestamp=1_000_000),
    ]
    monkeypatch.setattr(loop, "_stale_facebook_job", lambda *a: False)
    _run_once()
    assert seen[0]["published_at"] == 1000.0
    assert seen[1]["published_at"] == 5.0


class _StopCache:
    store: ClassVar[dict] = {}

    def get(self, key):
        return self.store.get(key)

    def exists(self, key):
        return key in self.store

    def set(self, key, value, ttl_seconds=None):
        self.store[key] = value


def _dispatch_setup(monkeypatch):
    _StopCache.store = {}
    runs: list[str] = []
    finished: list[str] = []

    async def fake_spider(request):
        runs.append(request["run_id"])

    monkeypatch.setattr(dispatch, "RedisCache", _StopCache)
    monkeypatch.setattr(task_queue, "RedisCache", _StopCache)
    monkeypatch.setattr(process, "RedisCache", _StopCache)
    monkeypatch.setattr(dispatch, "start_task", lambda request: None)
    monkeypatch.setattr(dispatch, "finish_task", lambda request, status, error=None: finished.append(status))
    monkeypatch.setattr(dispatch, "_run_spider", fake_spider)
    return runs, finished


def test_search_published_before_stop_is_skipped_after_drain_cleared(monkeypatch):
    runs, finished = _dispatch_setup(monkeypatch)
    _StopCache.store["platform_stop_cutoff:threads"] = 1000.0  # cờ drain đã bị một lần Run mới gỡ
    old = {"platform": "threads", "keyword": "k", "run_id": "old", "published_at": 999.0}
    new = {"platform": "threads", "keyword": "k", "run_id": "new", "published_at": 1001.0}
    assert asyncio.run(dispatch._handle_request(old)) is True
    assert asyncio.run(dispatch._handle_request(new)) is False
    assert runs == ["new"]
    assert finished == ["skipped", "done"]


def test_cutoff_falls_back_to_drain_value_and_ignores_plain_flag(monkeypatch):
    _dispatch_setup(monkeypatch)
    assert task_queue.stopped_at("threads") is None
    _StopCache.store["platform_drain:threads"] = 1
    assert task_queue.stopped_at("threads") is None
    _StopCache.store["platform_drain:threads"] = 2000.0
    assert task_queue.stopped_at("threads") == 2000.0
    _StopCache.store["platform_stop_cutoff:threads"] = 3000.0
    assert task_queue.stopped_at("threads") == 3000.0


def test_refresh_token_not_blocked_by_cutoff(monkeypatch):
    _, finished = _dispatch_setup(monkeypatch)
    ran = []

    async def fake_refresh(request):
        ran.append(request["run_id"])

    monkeypatch.setattr(dispatch, "_refresh_token", fake_refresh)
    _StopCache.store["platform_stop_cutoff:threads"] = 1000.0
    request = {"platform": "threads", "type": "refresh_token", "run_id": "r", "published_at": 999.0}
    asyncio.run(dispatch._handle_request(request))
    assert ran == ["r"] and finished == ["done"]


def test_finish_task_reports_comment_completion(monkeypatch):
    pushed = {}

    class Cache:
        def lrem_by_id(self, key, run_id):
            pass

        def lpush_capped(self, key, value, cap):
            pushed.setdefault(key, []).append(value)

    monkeypatch.setattr(task_queue, "RedisCache", Cache)
    task_queue.finish_task({"platform": "tiktok", "type": "comments", "post_id": "p1", "run_id": "r"}, "skipped")
    task_queue.finish_task({"platform": "tiktok", "keyword": "k", "run_id": "s"}, "done")
    completions = pushed[task_queue.COMMENT_COMPLETIONS_KEY]
    assert [(c["post_id"], c["status"]) for c in completions] == [("p1", "skipped")]
