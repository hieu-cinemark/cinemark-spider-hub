"""CRAWLER_PLATFORMS: mỗi máy chỉ chạy đúng vòng lặp được giao."""

import asyncio

import pytest

from social_crawler.consumer import common, loop


def test_empty_means_all(monkeypatch):
    monkeypatch.delenv("CRAWLER_PLATFORMS", raising=False)
    assert common.enabled_platforms() == list(common.PLATFORM_CONSUMER_GROUPS)


def test_unknown_platform_fails_instead_of_silently_dropping_it(monkeypatch):
    monkeypatch.setenv("CRAWLER_PLATFORMS", "facebook,tiktoks")
    with pytest.raises(ValueError, match="tiktoks"):
        common.enabled_platforms()


@pytest.mark.parametrize(
    "value,expected_platforms,workers",
    [
        ("facebook", ["facebook"], 0),
        ("facebook_public, threads,tiktok", ["facebook_public", "threads", "tiktok"], 2),
    ],
)
def test_run_starts_only_enabled_loops(monkeypatch, value, expected_platforms, workers):
    monkeypatch.setenv("CRAWLER_PLATFORMS", value)
    monkeypatch.setattr(loop.lanes, "LANES", 1)
    monkeypatch.setattr(loop.public_page_job, "MAX_WORKERS", 2)
    monkeypatch.setattr(loop.threads_channel_job, "MAX_WORKERS", 0)
    monkeypatch.setattr(loop, "_recover_threads_channel_queue", lambda: None)
    monkeypatch.setattr(loop, "_recover_public_pages_queue", lambda: None)
    started = []

    async def consumer(platform, group, lane=None):
        started.append(platform)

    async def worker(slot):
        started.append(f"worker{slot}")

    monkeypatch.setattr(loop, "_run_platform_consumer", consumer)
    monkeypatch.setattr(loop, "_run_public_pages_worker", worker)

    async def run_once():
        with pytest.raises(RuntimeError):  # vòng lặp kết thúc bị coi là bất thường
            await loop.run()
        await asyncio.sleep(0)

    asyncio.run(run_once())
    assert {name for name in started if not name.startswith("worker")} <= set(expected_platforms)
    assert sum(name.startswith("worker") for name in started) <= workers
