"""consumer.loop: làn ưu tiên Redis là hàng đợi tin cậy - job dở được trả lại khi khởi động, cạn proxy thì xếp lại."""

import asyncio
import json

from social_crawler.consumer import loop
from social_crawler.services import pool


class FakeCache:
    lists: dict[str, list[str]] = {}

    def __init__(self) -> None:
        pass

    def _l(self, key):
        return FakeCache.lists.setdefault(key, [])

    def claim_json(self, key, processing):
        src = self._l(key)
        if not src:
            return None, None
        raw = src.pop()  # RIGHT
        self._l(processing).insert(0, raw)
        return raw, json.loads(raw)

    def ack(self, processing, raw):
        self._l(processing).remove(raw)

    def requeue_processing(self, processing, key):
        moved = 0
        while self._l(processing):
            self._l(key).append(self._l(processing).pop())
            moved += 1
        return moved

    def lpush_json(self, key, value):
        self._l(key).insert(0, json.dumps(value))


def _setup(monkeypatch, handler):
    FakeCache.lists = {}
    monkeypatch.setattr(loop, "RedisCache", FakeCache)
    monkeypatch.setattr(loop, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(loop, "_handle_request", handler)
    monkeypatch.setattr(
        loop, "get_proxy_settings", lambda: {"exhausted_max_requeues": 2, "exhausted_backoff_base_seconds": 0.01}
    )

    async def no_sleep(platform, seconds):
        return None

    monkeypatch.setattr(loop, "_sleep_interruptible", no_sleep)


def test_done_jobs_are_acked(monkeypatch):
    seen = []

    async def handler(request):
        seen.append(request["run_id"])

    _setup(monkeypatch, handler)
    FakeCache.lists["crawl_priority:facebook"] = [json.dumps({"run_id": "b"}), json.dumps({"run_id": "a"})]
    asyncio.run(loop._drain_priority_queue("facebook"))
    assert seen == ["a", "b"]
    assert FakeCache.lists["crawl_priority:facebook:processing"] == []


def test_proxy_exhausted_job_is_requeued(monkeypatch):
    async def handler(request):
        raise pool.ProxyPoolExhaustedError("no proxy")

    _setup(monkeypatch, handler)
    FakeCache.lists["crawl_priority:facebook"] = [json.dumps({"run_id": "a"})]
    asyncio.run(loop._drain_priority_queue("facebook"))
    assert [json.loads(r) for r in FakeCache.lists["crawl_priority:facebook"]] == [
        {"run_id": "a", "_proxy_exhausted_retries": 1}
    ]
    assert FakeCache.lists["crawl_priority:facebook:processing"] == []


def test_unfinished_jobs_recovered_on_start(monkeypatch):
    _setup(monkeypatch, None)
    FakeCache.lists["crawl_priority:threads:processing"] = [json.dumps({"run_id": "x"})]
    loop._recover_priority_queue("threads")
    assert FakeCache.lists["crawl_priority:threads"] == [json.dumps({"run_id": "x"})]
    assert FakeCache.lists["crawl_priority:threads:processing"] == []


def test_idle_queue_runs_only_account_free_jobs(monkeypatch):
    seen = []

    async def handler(request):
        seen.append(request["type"])

    _setup(monkeypatch, handler)
    cache = FakeCache()
    cache.lpush_json("crawl_idle:facebook", {"type": "post_metrics", "run_id": "a"})
    cache.lpush_json("crawl_idle:facebook", {"type": "page_feed", "run_id": "b"})  # cần tài khoản -> bỏ

    assert asyncio.run(loop._run_one_idle_job("facebook")) is True
    assert asyncio.run(loop._run_one_idle_job("facebook")) is True
    assert asyncio.run(loop._run_one_idle_job("facebook")) is False  # hết job
    assert seen == ["post_metrics"]
    assert FakeCache.lists["crawl_idle:facebook:processing"] == []  # đều đã ack
