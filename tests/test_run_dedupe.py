"""consumer/dispatch: message Kafka giao lại với run_id đã chạy xong thì bỏ qua (31/10/2026)."""

import asyncio

from social_crawler.consumer import dispatch


class _Cache:
    store: dict = {}

    def exists(self, key):
        return key in self.store

    def set(self, key, value, ttl_seconds=None):
        self.store[key] = value


def test_redelivered_run_is_skipped(monkeypatch):
    _Cache.store = {}
    monkeypatch.setattr(dispatch, "RedisCache", _Cache)
    monkeypatch.setattr(dispatch, "_cancel_requested", lambda **kw: False)
    monkeypatch.setattr(dispatch, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(dispatch, "start_task", lambda request: None)
    monkeypatch.setattr(dispatch, "finish_task", lambda request, status, error=None: None)
    runs = []

    async def fake_nurture(request, bypass_drain=False):
        runs.append(request["run_id"])

    monkeypatch.setattr(dispatch, "_nurture_accounts", fake_nurture)
    request = {"platform": "facebook", "type": "nurture", "run_id": "r1"}
    assert asyncio.run(dispatch._handle_request(request)) is False
    assert asyncio.run(dispatch._handle_request(request)) is True  # giao lại: bỏ qua
    assert runs == ["r1"]


def test_unexpected_error_is_not_marked_done(monkeypatch):
    _Cache.store = {}
    monkeypatch.setattr(dispatch, "RedisCache", _Cache)
    monkeypatch.setattr(dispatch, "_cancel_requested", lambda **kw: False)
    monkeypatch.setattr(dispatch, "is_platform_draining", lambda platform: False)
    monkeypatch.setattr(dispatch, "start_task", lambda request: None)
    monkeypatch.setattr(dispatch, "finish_task", lambda request, status, error=None: None)

    async def boom(request, bypass_drain=False):
        raise RuntimeError("proxy exhausted")

    monkeypatch.setattr(dispatch, "_nurture_accounts", boom)
    request = {"platform": "facebook", "type": "nurture", "run_id": "r2"}
    try:
        asyncio.run(dispatch._handle_request(request))
    except RuntimeError:
        pass
    assert not dispatch._already_done("r2")  # xếp lại cùng run_id vẫn chạy được
