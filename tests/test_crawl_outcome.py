"""core/crawl_outcome + consumer/process: mã thoát 0 của Scrapy không đủ - consumer đọc kết quả có cấu trúc của lượt crawl.
Các test tiến trình thật chạy spider tổng hợp, không gửi request nào."""

from __future__ import annotations

import asyncio
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from social_crawler.consumer import common, process
from social_crawler.core.crawl_outcome import CANCELLED, FAILED, PARTIAL, SUCCESS, classify


def _classify(**overrides):
    return classify(
        **{
            "finish_reason": "finished",
            "engine_errors": 0,
            "spider_exceptions": 0,
            "published": 3,
            "publish_failed": 0,
            **overrides,
        }
    )


def test_classify():
    assert _classify()["status"] == SUCCESS
    assert _classify(published=0)["status"] == SUCCESS  # không có bài mới không phải lỗi
    assert _classify(engine_errors=1)["status"] == FAILED
    assert _classify(finish_reason="bandwidth_exceeded")["status"] == FAILED
    assert _classify(spider_exceptions=2)["status"] == PARTIAL
    assert _classify(finish_reason="shutdown")["status"] == CANCELLED
    assert _classify(publish_failed=1)["status"] == PARTIAL
    assert _classify(publish_failed=1)["retryable"] is True
    assert _classify(finish_reason="shutdown", publish_failed=1)["retryable"] is False


def _spider(tmp_path, body: str) -> list[str]:
    path = tmp_path / "synthetic_spider.py"
    path.write_text(
        "import scrapy\n\nclass S(scrapy.Spider):\n    name = 's'\n" + textwrap.indent(textwrap.dedent(body), "    ")
    )
    return [common.SCRAPY_BIN, "runspider", str(path), "-s", "TELNETCONSOLE_ENABLED=False", "-s", "LOG_LEVEL=CRITICAL"]


def test_start_exception_with_exit_zero_is_failed(tmp_path):
    """Đúng phép tái hiện của review 02/11/2026: start() raise, scrapy thoát 0."""
    args = _spider(
        tmp_path,
        """
        async def start(self):
            raise RuntimeError("boom")
            yield
        """,
    )
    assert asyncio.run(process._run_subprocess(args)) == common.CRAWL_OUTCOME_FAILED_EXIT_CODE


def test_clean_spider_is_success(tmp_path):
    args = _spider(
        tmp_path,
        """
        async def start(self):
            return
            yield
        """,
    )
    assert asyncio.run(process._run_subprocess(args)) == 0


def test_manual_run_without_outcome_file_still_works(tmp_path, monkeypatch):
    """`scrapy crawl` chạy tay không có CRAWL_OUTCOME_FILE: extension tự tắt, không làm hỏng lệnh."""
    args = _spider(
        tmp_path,
        """
        async def start(self):
            return
            yield
        """,
    )
    monkeypatch.delenv("CRAWL_OUTCOME_FILE", raising=False)
    result = subprocess.run(args, cwd=common.REPO_ROOT, capture_output=True, timeout=60, check=False)
    assert result.returncode == 0, result.stderr[-800:]


def test_sys_exit_in_start_keeps_its_code(tmp_path):
    args = _spider(
        tmp_path,
        """
        async def start(self):
            import sys
            sys.exit(75)
            yield
        """,
    )
    assert asyncio.run(process._run_subprocess(args)) == 75


def test_stop_signal_while_crawling_is_cancelled_not_done(tmp_path, monkeypatch):
    """Nút Dừng: Scrapy tắt gọn và thoát 0 - không được tính là job đã xong."""
    started = time.monotonic()

    class Cache:
        def exists(self, key):
            return time.monotonic() - started > 3

    monkeypatch.setattr(process, "RedisCache", Cache)
    args = _spider(
        tmp_path,
        """
        async def start(self):
            import asyncio
            await asyncio.sleep(120)
            return
            yield
        """,
    )
    assert asyncio.run(process._run_subprocess(args, run_id="r")) == common.CRAWL_OUTCOME_CANCELLED_EXIT_CODE
    assert time.monotonic() - started < 30


def test_missing_outcome_file_is_failed(tmp_path):
    assert process._apply_crawl_outcome(0, tmp_path / "none.json", ["scrapy"]) == common.CRAWL_OUTCOME_FAILED_EXIT_CODE
    assert process._apply_crawl_outcome(75, tmp_path / "none.json", ["scrapy"]) == 75


@pytest.mark.parametrize(
    ("returncode", "error"),
    [
        (common.CRAWL_OUTCOME_FAILED_EXIT_CODE, common.CrawlJobFailed),
        (common.CRAWL_OUTCOME_RETRYABLE_EXIT_CODE, common.CrawlJobRetryable),
        (common.CRAWL_OUTCOME_CANCELLED_EXIT_CODE, common.CrawlJobSkipped),
    ],
)
def test_outcome_codes_raise_for_jobs(returncode, error):
    with pytest.raises(error):
        common._raise_for_returncode(returncode, "ctx")


def _alive(pid: int) -> bool:
    import os

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def test_task_cancel_stops_subprocess_group():
    """Huỷ task điều phối: tiến trình con không còn sống sau khi CancelledError lan ra."""

    async def run() -> int:
        spawned: list[asyncio.subprocess.Process] = []
        real_exec = asyncio.create_subprocess_exec

        async def spy(*args, **kwargs):
            proc = await real_exec(*args, **kwargs)
            spawned.append(proc)
            return proc

        asyncio.create_subprocess_exec = spy
        try:
            task = asyncio.create_task(process._run_subprocess([sys.executable, "-c", "import time; time.sleep(60)"]))
            while not spawned:
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            asyncio.create_subprocess_exec = real_exec
        assert spawned[0].returncode is not None
        return spawned[0].pid

    assert not _alive(asyncio.run(run()))


def test_second_cancel_during_cleanup_does_not_abandon_child(monkeypatch):
    monkeypatch.setattr(process, "JOB_CANCEL_KILL_GRACE_SECONDS", 0.5)
    # Con bỏ qua SIGTERM: phải chờ hết grace rồi bị SIGKILL, kể cả khi bị huỷ lần nữa trong lúc đó.
    code = (
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print('ready', flush=True)\n"
        "time.sleep(60)\n"
    )

    async def run() -> asyncio.subprocess.Process:
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-c", code, start_new_session=True, stdout=asyncio.subprocess.PIPE
        )
        await proc.stdout.readline()

        async def waiter():
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                await process._terminate_on_cancel(proc)
                raise

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        task.cancel()
        await asyncio.sleep(0.05)
        task.cancel()
        started = time.monotonic()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert time.monotonic() - started < 5
        return proc

    proc = asyncio.run(run())
    assert proc.returncode == -signal.SIGKILL
