"""Chạy tiến trình con (spider / bootstrap), chuyển log, huỷ theo run_id hoặc theo nền tảng."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import (
    CRAWL_JOB_CANCEL_KEY_TMPL,
    CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL,
    JOB_CANCEL_KILL_GRACE_SECONDS,
    JOB_CANCEL_POLL_SECONDS,
    REPO_ROOT,
    logger,
)
from social_crawler.logger import file_logging_enabled, write_passthrough
from social_crawler.services.task_queue import is_platform_draining, stopped_at


async def _run_subprocess(
    args: list[str],
    *,
    run_id: str | None = None,
    platform: str | None = None,
    honor_platform_cancel: bool = True,
) -> int:
    """Chạy args thành tiến trình con, để stdout/stderr chảy thẳng ra stdout của tiến trình
    này, và trả về mã thoát.

    run_id và/hoặc platform, khi có, cho phép huỷ từ bên ngoài: kiểm tra định kỳ
    CRAWL_JOB_CANCEL_KEY_TMPL / CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL (do POST
    /<platform>/stop của cinemark-api đặt) và gửi signal cho cả nhóm tiến trình khi một
    trong hai key xuất hiện. Không có cái nào thì chỉ chờ bình thường.

    honor_platform_cancel=False dành cho refresh_token / cookie_import: Dừng để nguyên
    crawl_job_cancel_platform để các lượt crawl còn sót chết đi, nhưng một lần
    Restore/refresh sau đó vẫn phải chạy. Bấm Dừng trong lúc đang refresh vẫn có tác dụng
    qua crawl_job_cancel:<run_id>."""
    # Nhóm tiến trình riêng để SIGTERM/SIGKILL tới được Scrapy *và* các trình duyệt lồng bên
    # trong (Patchright/Chrome) thay vì để lại tiến trình mồ côi.
    #
    # Khi consumer ghi log ra file (enable_file_logging, xem __main__), output của tiến trình
    # con - nơi phần lớn lỗi crawl thật sự xảy ra - được đọc qua pipe và chuyển tiếp qua
    # cùng đích đó; để stdout kế thừa thẳng thì nó chỉ hiện ở terminal và trang Nhật ký của
    # dashboard (đọc consumer.log) không bao giờ thấy.
    relay_output = file_logging_enabled()
    env = None
    if relay_output and sys.stdout.isatty():
        # Output con đi qua pipe nên con tự tắt màu - ép bật lại cho terminal; file log vẫn sạch
        # (xem logger._use_colors).
        env = {**os.environ, "LOG_COLOR": "1"}
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=REPO_ROOT,
        env=env,
        start_new_session=True,
        stdout=asyncio.subprocess.PIPE if relay_output else None,
        stderr=asyncio.subprocess.STDOUT if relay_output else None,
        limit=_RELAY_LINE_LIMIT,
    )
    relay = asyncio.create_task(_relay_output(process.stdout)) if relay_output and process.stdout else None
    try:
        return await _wait_for_subprocess(
            process, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
        )
    finally:
        if relay is not None:
            try:
                await asyncio.wait_for(relay, timeout=10)
            except TimeoutError, asyncio.CancelledError:
                relay.cancel()


# Giới hạn một dòng của StreamReader (mặc định 64KB). Một dòng dài hơn (dump HTML/JSON khi
# debug) làm readline() raise - relay phải đọc tiếp bằng khối thay vì dừng, vì pipe không
# ai đọc sẽ đầy và chặn luôn tiến trình crawl con.
_RELAY_LINE_LIMIT = 1024 * 1024


async def _relay_output(stream: asyncio.StreamReader) -> None:
    while True:
        try:
            chunk = await stream.readline()
        except asyncio.LimitOverrunError, ValueError:
            chunk = await stream.read(_RELAY_LINE_LIMIT)
        if not chunk:
            return
        write_passthrough(chunk.decode("utf-8", errors="replace"))


async def _wait_for_subprocess(
    process: asyncio.subprocess.Process,
    *,
    run_id: str | None,
    platform: str | None,
    honor_platform_cancel: bool,
) -> int:
    if run_id is None and platform is None:
        return await process.wait()

    cache = RedisCache()
    cancel_key = CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id) if run_id else None
    platform_cancel_key = CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL.format(platform=platform) if platform else None

    def _signal_group(sig: signal.Signals) -> None:
        try:
            os.killpg(process.pid, sig)
        except ProcessLookupError:
            pass

    while True:
        try:
            return await asyncio.wait_for(process.wait(), timeout=JOB_CANCEL_POLL_SECONDS)
        except TimeoutError:
            cancelled = cancel_key is not None and cache.exists(cancel_key)
            if (
                not cancelled
                and honor_platform_cancel
                and platform_cancel_key is not None
                and cache.exists(platform_cancel_key)
            ):
                cancelled = True
            if not cancelled:
                continue
            logger.warning("crawl_job_cancel_requested", run_id=run_id, platform=platform)
            _signal_group(signal.SIGTERM)
            try:
                return await asyncio.wait_for(process.wait(), timeout=JOB_CANCEL_KILL_GRACE_SECONDS)
            except TimeoutError:
                logger.warning("crawl_job_force_killed", run_id=run_id, platform=platform)
                _signal_group(signal.SIGKILL)
                return await process.wait()


async def _sleep_interruptible(platform: str, seconds: float) -> bool:
    """Ngủ tối đa `seconds`, trả về True ngay khi Dừng bật drain để message Kafka kế tiếp bị bỏ
    qua thay vì chờ hết khoảng nghỉ."""
    deadline = time.monotonic() + seconds
    while True:
        if is_platform_draining(platform):
            return True
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        await asyncio.sleep(min(JOB_CANCEL_POLL_SECONDS, remaining))


def _published_before_stop(request: dict[str, Any], platform: str) -> bool:
    """Message này có được publish trước lần bấm Dừng gần nhất của nền tảng không (xem
    services/task_queue.stopped_at). Message từ cinemark-api cũ không mang published_at và
    không bao giờ bị coi là cũ."""
    stop_at = stopped_at(platform)
    published_at = request.get("published_at")
    return stop_at is not None and isinstance(published_at, (int, float)) and published_at < stop_at


def _cancel_requested(*, run_id: str | None = None, platform: str | None = None) -> bool:
    cache = RedisCache()
    if run_id and cache.exists(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id)):
        return True
    if platform and cache.exists(CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL.format(platform=platform)):
        return True
    return False


def _stopped_for(*, run_id: str | None, platform: str, bypass_drain: bool) -> bool:
    """Bản biết bypass của phép kiểm tra `_cancel_requested(...) or is_platform_draining(...)`
    mà comments/channel_videos/nurture đều kiểm tra định kỳ trước/trong tiến trình con của
    mình. Lệnh huỷ theo run_id cụ thể (một lần bấm Dừng tới *sau* khi chính lượt chạy này đã
    bắt đầu) luôn có hiệu lực. Các tín hiệu cho cả nền tảng (crawl_job_cancel_platform /
    platform_drain / comments_drain - đều do một lần Dừng trước đó bật, xem
    crawl_jobs.request_stop của cinemark-api) bị bỏ qua khi có bypass_drain - xem docstring
    của _handle_request để biết vì sao một hành động lẻ như vậy không được thừa hưởng một lần
    Dừng cũ dành cho một hàng tồn khác, đã xả xong."""
    if _cancel_requested(run_id=run_id):
        return True
    if bypass_drain:
        return False
    return _cancel_requested(platform=platform) or is_platform_draining(platform)
