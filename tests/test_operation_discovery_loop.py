"""Dò operation Relay không được chạy V8 ngay trong vòng lặp asyncio đang chạy (mini-racer 0.14.1 ném AssertionError)."""

import asyncio
import threading

from social_crawler.spiders.facebook.auth import operation_fallback


def _probe(monkeypatch):
    seen = {}

    def fake(fetch, pages, targets, *, deadline=None):
        seen["thread"] = threading.current_thread()
        try:
            asyncio.get_running_loop()
            seen["loop_running"] = True
        except RuntimeError:
            seen["loop_running"] = False
        return {"X": "1"}

    monkeypatch.setattr(operation_fallback, "_discover", fake)
    return seen


def test_discover_inside_a_running_loop_uses_a_worker_thread_without_a_loop(monkeypatch):
    seen = _probe(monkeypatch)

    async def run():
        return operation_fallback.discover(lambda *a: "", [], {"X"})

    assert asyncio.run(run()) == {"X": "1"}
    assert seen["loop_running"] is False and seen["thread"] is not threading.main_thread()


def test_discover_without_a_loop_runs_in_place(monkeypatch):
    seen = _probe(monkeypatch)
    assert operation_fallback.discover(lambda *a: "", [], {"X"}) == {"X": "1"}
    assert seen["thread"] is threading.current_thread()


def test_real_v8_runtime_starts_through_discover_inside_a_loop():
    async def run():
        return operation_fallback.discover(lambda page, asset, deadline: "", [], {"Nothing"})

    assert asyncio.run(run()) == {}
