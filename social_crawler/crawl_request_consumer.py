"""Lắng nghe topic crawl_requests trên Kafka và khởi chạy tiến trình con tương ứng cho mỗi
request - phía consumer của các endpoint "chạy crawl"/"refresh token" bấm tay và job
theo lịch hằng ngày của cinemark-api (xem app/clients/kafka.py +
app/api/routes/platform_scraper.py của cinemark-api).

Chạy mỗi nền tảng một vòng lặp consumer độc lập (xem PLATFORM_CONSUMER_GROUPS), mỗi vòng
trong consumer group Kafka riêng cùng đọc topic crawl_requests - group nào cũng thấy mọi
message, nhưng bỏ qua ngay (commit qua) những gì không phải nền tảng của mình. Đây là chủ
đích, không phải sơ suất: trong một nền tảng, các request vẫn được xử lý đúng từng cái
một (bắn nhiều tiến trình con cùng lúc vào cùng một tài khoản/session đúng là kiểu dồn
dập mà cơ chế bóp nhịp/jitter ở chỗ khác trong project được thiết kế để tránh) - nhưng
lý do đó không liên quan gì tới tài khoản/session/proxy hoàn toàn riêng của một nền tảng
*khác*, nên một lượt crawl Facebook chậm hay bị kẹt không bao giờ được làm trễ một
request Threads hay TikTok nằm trong cùng topic. Dùng ba consumer group thay vì ba topic
giúp đây chỉ là thay đổi phía spider-hub - phía producer của cinemark-api vẫn publish
lên một topic dùng chung, không biết có gì thay đổi ở phía này.

Chạy bằng:
    python -m social_crawler.crawl_request_consumer
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sys
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from social_crawler.clients.kafka import CRAWL_REQUESTS_TOPIC, KafkaPublisher
from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
    CACHE_REDIS_KEY_TMPL,
    COMMENTS_REDIS_KEY_TMPL,
    DEFAULT_ACCOUNT_KEY,
    REPLIES_REDIS_KEY_TMPL,
)
from social_crawler.constants.threads import (
    ACTIVE_ACCOUNT_REDIS_KEY as THREADS_ACTIVE_ACCOUNT_REDIS_KEY,
)
from social_crawler.constants.threads import (
    CACHE_REDIS_KEY_TMPL as THREADS_CACHE_REDIS_KEY_TMPL,
)
from social_crawler.constants.threads import (
    DEFAULT_ACCOUNT_KEY as THREADS_DEFAULT_ACCOUNT_KEY,
)
from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE
from social_crawler.db.proxy_settings import get_proxy_settings
from social_crawler.logger import enable_file_logging, file_logging_enabled, get_logger, write_passthrough
from social_crawler.services import pool
from social_crawler.services.task_queue import finish_task, is_platform_draining, start_task, stopped_at
from social_crawler.spiders.comet_graphql_client import is_search_recipe

logger = get_logger(__name__)

# Mỗi nền tảng một consumer group - xem docstring module để biết lý do. Các key khớp với
# mọi nền tảng có thể xuất hiện trong trường "platform" của một crawl request (mỗi cái cần
# vòng lặp độc lập riêng ở đây). Crawl hashtag TikTok dùng chung group "tiktok" với
# comments/refresh/nurture của nền tảng đó.
#
# Ghi chú vận hành cho người tiếp theo đổi tên một trong các group này (hoặc thêm nền tảng
# mới): một group id hoàn toàn mới chưa có offset nào được commit, và
# auto_offset_reset="earliest" bên dưới nghĩa là nó bắt đầu từ message cũ nhất còn giữ
# trên crawl_requests - phát lại mọi request đã xử lý trong khoảng đó như request mới
# (tiến trình scrapy/refresh token bị trùng). Trước khi đổi tên, seek group id mới về
# "latest" trước:
#   kafka-consumer-groups.sh --bootstrap-server <host> --group <new-group> \
#     --topic crawl_requests --reset-offsets --to-latest --execute
# (đã làm cho spider-hub.crawl-requests.{facebook,threads,tiktok} khi tách từ group duy
# nhất "spider-hub.crawl-requests".)
PLATFORM_CONSUMER_GROUPS = {
    "facebook": "spider-hub.crawl-requests.facebook",
    "threads": "spider-hub.crawl-requests.threads",
    "tiktok": "spider-hub.crawl-requests.tiktok",
}

# Khi một lần kích hoạt theo lô (ví dụ cron theo lịch) xếp hàng nhiều từ khoá cùng lúc,
# việc xử lý đúng từng cái một của consumer này (xem docstring module) sẽ để chúng bắn
# liên tiếp không có khoảng nghỉ nào - cùng dấu hiệu bot "đều tăm tắp/liên tục" mà jitter
# theo từng request ở chỗ khác trong project đã chặn, chỉ là ở cấp giữa các lượt crawl thay
# vì giữa các trang. Cùng khoảng 5-20s như sweep_pause_min/max của Facebook (search.py) -
# dùng lại một giá trị đã chứng minh là hợp lý thay vì nghĩ ra cái thứ hai.
INTER_REQUEST_PAUSE_MIN_SECONDS = 5.0
INTER_REQUEST_PAUSE_MAX_SECONDS = 20.0

# Một request thất bại vì pool.acquire_proxy_for_account(required=True) thấy cả pool
# proxy của nền tảng đã cạn (xem docstring của lỗi đó - proxy của một tài khoản đã ghim
# đang cooldown và cũng không có proxy thay thế nào khoẻ) KHÔNG phải "một request lỗi" như
# payload sai định dạng hay tài khoản chết - mọi request *khác* của nền tảng này nằm sau
# nó trong topic đều đang lao vào đúng bức tường đó cho tới khi hết cooldown (khoảng thực
# tế đã thấy: 10 phút tới 2 giờ, xem lịch backoff của db.record_proxy_outcome). Trước khi
# có backoff này, vòng lặp nền tảng cứ commit qua từng cái hết tốc lực - đã xác nhận thực
# tế (2026-09-17) đốt sạch cả một hàng tồn comment trong vài giây, mỗi lần thử là một lần
# khởi động/dừng spider tức thì, chắc chắn thất bại, cho tới tận khi cooldown tự hết.
#
# Ngoài bản thân backoff, riêng loại lỗi này được publish lại lên crawl_requests (xem
# _requeue_after_proxy_exhaustion bên dưới) thay vì chỉ log rồi để bị commit-và-mất như mọi
# exception khác ở đây - đây là kiểu lỗi duy nhất mà consumer này xác định chắc chắn được là
# "bản thân request không sao, chỉ là nền tảng chưa phục vụ được *ngay lúc này*", nên một
# lần thử lại thật (không chỉ "ngừng dồn dập") là đáng làm. Giới hạn tối đa
# MAX_PROXY_EXHAUSTED_REQUEUES lần publish lại (đếm trong trường
# "_proxy_exhausted_retries" của chính request) để một nền tảng có pool proxy chết hẳn
# (không chỉ đang cooldown) không lặp mãi - cuối cùng nó bị bỏ thật, có báo rõ ràng. Mức
# backoff cơ sở/hệ số tăng/tối đa và trần số lần xếp hàng lại là các giá trị exhausted_*
# trong proxy_settings của dashboard (mặc định 30s / x2 / 300s / 3).

# Session Kafka của một nền tảng bị thu hồi (broker restart, mạng chập chờn, rebalance
# đua với consumer.commit() thành CommitFailedError/IllegalGenerationError - tất cả đều
# rơi vào handler KafkaError của _run_platform_consumer_once) là lỗi tạm thời và hồi phục
# được chỉ bằng cách vào lại group với một consumer mới. Thử lại ở đây trước thay vì rơi
# ngay xuống phản ứng cuối cùng của run(), vốn huỷ luôn vòng lặp của *hai* nền tảng kia và
# thoát cả tiến trình để một trình giám sát bên ngoài khởi động lại - quá tay với một trục
# trặc chỉ của một nền tảng, và trong môi trường dev không có trình giám sát như vậy thì nó
# chỉ để lại tình trạng không còn gì consume crawl_requests cho tới khi có người để ý. Chỉ
# khi broker vẫn không truy cập được quá _CONSUMER_RESTART_MAX_ATTEMPTS lần khởi động lại
# trong _CONSUMER_RESTART_WINDOW_SECONDS thì mới bỏ cuộc và rơi xuống phản ứng nặng hơn đó.
_CONSUMER_RESTART_BACKOFF_BASE_SECONDS = 5.0
_CONSUMER_RESTART_BACKOFF_GROWTH_FACTOR = 2.0
_CONSUMER_RESTART_BACKOFF_MAX_SECONDS = 120.0
_CONSUMER_RESTART_MAX_ATTEMPTS = 5
_CONSUMER_RESTART_WINDOW_SECONDS = 600.0

SCRAPY_BIN = str(Path(sys.executable).parent / "scrapy")
PYTHON_BIN = sys.executable

REPO_ROOT = Path(__file__).resolve().parent.parent

SPIDER_BY_PLATFORM = {"facebook": "facebook_search", "threads": "threads_search"}
TIKTOK_HASHTAG_SPIDER = "tiktok_hashtag_search"
TIKTOK_CHANNEL_VIDEOS_SPIDER = "tiktok_channel_videos"


class CrawlJobFailed(Exception):
    """Tiến trình con kết thúc không thành công. Consumer ghi `failed` và chạy tiếp - đây không
    phải crash của vòng lặp nền tảng."""


class CrawlJobSkipped(Exception):
    """Request không được tính là một lượt crawl đã hoàn thành (BFS drain, huỷ trước khi spider
    bắt đầu)."""


def _raise_for_returncode(returncode: int, context: str) -> None:
    if returncode == PROXY_EXHAUSTED_EXIT_CODE:
        logger.warning("subprocess_proxy_exhausted", context=context, returncode=returncode)
        raise pool.ProxyPoolExhaustedError(context)
    if returncode != 0:
        logger.error("subprocess_failed", context=context, returncode=returncode)
        raise CrawlJobFailed(f"{context} (exit {returncode})")


# Mọi nền tảng có spider comments/replies riêng (xem
# social_crawler/spiders/<platform>/features/comments/).
COMMENTS_SPIDER_BY_PLATFORM = {
    "facebook": "facebook_comments",
    "threads": "threads_comments",
    "tiktok": "tiktok_comments",
}
# Facebook vẫn cần cache GraphQL của query comment. Threads chỉ cần cookie session tìm
# kiếm (reply qua REST text_feed). TikTok tự ký các request curl_cffi bằng danh tính
# synthetic cho mỗi lượt crawl (xem docstring module của
# spiders/tiktok/features/comments/comments.py - không dùng trình duyệt từ 2026-09-18),
# nên cũng không có trong tập này.
COMMENTS_PLATFORMS_NEEDING_CACHE = {"facebook", "threads"}
COMMENTS_BOOTSTRAP_MODULE = {
    "facebook": "social_crawler.spiders.facebook.auth.bootstrap",
    "threads": "social_crawler.spiders.threads.auth.bootstrap",
}

# Mọi nền tảng có cache token bootstrap bằng trình duyệt riêng (xem constants/facebook.py
# + constants/threads.py) - một request refresh_token chỉ định cái nào qua "platform" (mặc
# định facebook cho mọi message cũ/đã xếp hàng từ trước khi hỗ trợ nhiều nền tảng).
TOKEN_REFRESH_BOOTSTRAP_MODULE = {
    "facebook": "social_crawler.spiders.facebook.auth.bootstrap",
    "threads": "social_crawler.spiders.threads.auth.bootstrap",
    "tiktok": "social_crawler.spiders.tiktok.auth.bootstrap",
}

TOKEN_REFRESH_QUERY = "tin tức hôm nay"

DEFAULT_SWEEP_DAYS = 60  # khoảng 2 tháng


def _sweep_days_for(request: dict[str, Any]) -> int:
    """facebook_search nên quét bao nhiêu ngày. Spider neo các khoảng thời gian ở end_date
    (hoặc hôm nay) và lùi lại sweep_days (xem _date_windows). Một khoảng ngày người dùng chọn
    được quy ra số ngày đó thay vì một query không lọc duy nhất mà Facebook sẽ giới hạn.
    Không có khoảng ngày - DEFAULT_SWEEP_DAYS."""
    start_raw, end_raw = request.get("start_date"), request.get("end_date")
    if start_raw and end_raw:
        start, end = date.fromisoformat(start_raw), date.fromisoformat(end_raw)
        return max((end - start).days + 1, 1)
    if start_raw:
        return max((date.today() - date.fromisoformat(start_raw)).days + 1, 1)
    return DEFAULT_SWEEP_DAYS


CRAWL_JOB_KEY_TMPL = "crawl_job:{platform}"
CRAWL_JOB_CANCEL_KEY_TMPL = "crawl_job_cancel:{run_id}"
CRAWL_JOB_CANCEL_PLATFORM_KEY_TMPL = "crawl_job_cancel_platform:{platform}"
# refresh_tracker của dashboard trước đây chỉ đọc dần consumer.log, nhưng consumer chạy
# trong terminal (stdout, không tee) không bao giờ ghi file đó - panel cứ ở "running" với 0
# dòng kể cả sau token_refresh_finished. Key này là tín hiệu xong bền vững (cùng prefix với
# mọi key RedisCache khác). TTL đủ cho dashboard kết nối lại chậm mà không để lại rác.
REFRESH_RESULT_KEY_TMPL = "token_refresh_result:{run_id}"
REFRESH_RESULT_TTL_SECONDS = 600
# Tần suất _run_subprocess kiểm tra yêu cầu huỷ trong khi tiến trình crawl con đang chạy -
# đủ ngắn để nút Dừng trên dashboard phản hồi nhanh, đủ dài để không dồn Redis cho một job
# thường chạy vài phút.
JOB_CANCEL_POLL_SECONDS = 0.25
# SIGTERM trước (reactor Twisted của Scrapy bắt nó và tắt spider gọn gàng - đóng Kafka
# producer, flush log), chỉ SIGKILL nếu cách đó không kịp.
JOB_CANCEL_KILL_GRACE_SECONDS = 10.0


def _mark_refresh_result(run_id: str | None, *, ok: bool) -> None:
    if not run_id:
        return
    RedisCache().set(
        REFRESH_RESULT_KEY_TMPL.format(run_id=run_id),
        {"ok": ok},
        ttl_seconds=REFRESH_RESULT_TTL_SECONDS,
    )


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


def _facebook_session_is_cached(account: str | None = None) -> bool:
    """`account` (hoặc tài khoản Facebook đang active, hoặc mặc định nếu bootstrap chưa từng
    chạy) còn cache token sống trong Redis không. Chính việc Redis cho key hết hạn *là* tín
    hiệu "hết hạn" - graphql_client.py raise SessionExpiredError ngay khi key này mất, nên chỉ
    cần kiểm tra tồn tại, không cần tính TTL riêng.

    `account`, khi có, kiểm tra ĐÚNG tài khoản đó thay vì tài khoản nào đang active - xem
    comment trong _run_comments_spider để biết vì sao một job crawl comment ghim một tài
    khoản cho cả ba lời gọi ensure_* thay vì để mỗi lời gọi tự đọc/xoay
    ACTIVE_ACCOUNT_REDIS_KEY."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    session = cache.get(CACHE_REDIS_KEY_TMPL.format(account=target))
    if session and not is_search_recipe(session):
        # Key search đang giữ công thức comment (xem comet_graphql_client.is_search_recipe) - coi như
        # chưa có để bootstrap search lại.
        logger.warning(
            "facebook_session_cache_not_search",
            account=target,
            friendly_name=session.get("fb_api_req_friendly_name"),
        )
        return False
    return bool(session)


async def _ensure_facebook_session(
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str | None = None,
    honor_platform_cancel: bool = True,
) -> bool:
    """Làm mới cache token Facebook trước nếu nó thiếu/hết hạn, để một lượt crawl kích hoạt
    ngay sau khoảng nhàn rỗi >6 giờ (xem CACHE_MAX_AGE_SECONDS) thành công ngay lần đầu thay
    vì lỗi session_expired rồi phải chờ lượt kế tiếp của scripts/refresh_token.sh hoặc kích
    hoạt lại bằng tay. Trả về có session để crawl sau khi hàm này xong không - False nghĩa là
    bản thân lần làm mới thất bại, nên chỗ gọi nên bỏ cuộc thay vì chạy một lượt crawl chắc
    chắn dính ngay lỗi session_expired đó.

    `account`, khi có, được truyền cho bootstrap.py dưới dạng --account - làm mới/kiểm tra
    đúng tài khoản đó thay vì để next_account() của bootstrap.py tự do xoay vòng (xem
    _run_comments_spider)."""
    # Chạy ngoài event loop: RedisCache bọc client `redis` đồng bộ, và loop này giờ dùng chung
    # với vòng lặp consumer của threads/tiktok (xem run()) - một lời gọi Redis chậm/treo ở đây
    # sẽ làm treo luôn việc poll Kafka của chúng, đúng kiểu dính chéo giữa các nền tảng mà việc
    # tách mỗi nền tảng một vòng lặp sinh ra để loại bỏ.
    if await asyncio.to_thread(_facebook_session_is_cached, account):
        return True

    logger.info("facebook_session_expired_refreshing_first", account=account)
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.facebook.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
    if account:
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0 and account and returncode > 0:
        # Tài khoản được ghim (thường là con trỏ active) không làm mới được - bootstrap đã đánh dấu nó
        # chết và đưa vào cooldown. Thử một lần không ghim để next_account() chọn tài khoản khoẻ
        # khác, thay vì để mọi job sau cứ bootstrap lại đúng tài khoản chết này. returncode âm là bị
        # huỷ (bấm Dừng) - không thử lại.
        logger.warning("facebook_session_refresh_failed_trying_other_account", account=account, returncode=returncode)
        args = [PYTHON_BIN, "-m", "social_crawler.spiders.facebook.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
        )
    if returncode != 0:
        logger.error("facebook_session_refresh_before_crawl_failed", returncode=returncode)
        return False
    return True


def _facebook_comments_cache_usable(account: str | None = None) -> bool:
    """`account` (hoặc tài khoản Facebook đang active) có cache query comment trong Redis thực sự
    phân trang được không - xem FacebookGraphQLClient._get_comments_cache. Khác cache tìm
    kiếm (_facebook_session_is_cached), mẫu query này dùng được cho comment của *bất kỳ* bài
    nào sau khi đã bắt được (chỉ biến `id` thay đổi theo bài - xem get_comments trong
    graphql_client.py), nên chỉ cần bootstrap một lần mỗi TTL, không phải mỗi bài.

    Kiểm tra sub-key `pagination`, không chỉ xem cache có tồn tại không - bootstrap.py chỉ
    bắt được CommentsListComponentsPaginationQuery nếu bài nó cuộn trong lượt đó thực sự có
    đủ comment để Facebook trả trang thứ hai (xem comments_trigger); một cache bootstrap trên
    bài ít comment vẫn tồn tại nhưng không bao giờ lấy được quá khoảng 2 comment cho bất kỳ
    bài nào, một cách âm thầm, cho tới khi lần bootstrap sau tình cờ gặp bài nhiều comment
    (đã xảy ra thật: mọi lượt crawl comment chỉ được tối đa khoảng 2 suốt nhiều giờ chính vì
    chuyện này). Kiểm tra lại ở mỗi lời gọi thay vì chỉ tin vào việc tồn tại nghĩa là một
    cache tồi sẽ được thử lại thay vì kẹt tới khi hết TTL."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    comments_cache = cache.get(COMMENTS_REDIS_KEY_TMPL.format(account=target))
    return bool(comments_cache and comments_cache.get("pagination"))


def _facebook_replies_cache_usable(account: str | None = None) -> bool:
    """`account` (hoặc tài khoản Facebook đang active) có cache query reply trong Redis thực sự
    phân trang được không - cùng lý do và cùng yêu cầu sub-key `pagination` như
    _facebook_comments_cache_usable, chỉ là cho get_replies/get_replies_next_page của
    _fetch_replies thay vì vòng lặp comment cấp một. Là cache *riêng* với cache comment
    (REPLIES_REDIS_KEY_TMPL, không phải COMMENTS_REDIS_KEY_TMPL) vì bắt nó cần một cách kích
    hoạt trình duyệt khác ("bấm mở reply của một comment" của replies_trigger, không phải "chỉ
    mở+sắp xếp danh sách cấp một" của comments_trigger) - đã xảy ra thật (2026-09-16): một
    lần bootstrap chỉ cho comment khiến mọi comment có reply trong một lượt crawl thật đều
    lỗi session_expired ở đúng cache này, dù bản thân comment cấp một vẫn lấy bình thường."""
    cache = RedisCache()
    target = account or cache.get(ACTIVE_ACCOUNT_REDIS_KEY) or DEFAULT_ACCOUNT_KEY
    replies_cache = cache.get(REPLIES_REDIS_KEY_TMPL.format(account=target))
    return bool(replies_cache and replies_cache.get("pagination"))


def _threads_search_cache_usable() -> bool:
    """Tài khoản Threads đang active có cache session *tìm kiếm* dùng được trong Redis không.
    Dùng cho cả threads_search lẫn threads_comments: spider comment chỉ cần cookie của session
    (nó GET /api/v1/text_feed/<id>/replies/, xem graphql_client.get_text_feed_replies), mà
    bootstrap search tạo ra đủ cả cookie lẫn công thức search đúng.

    Không chỉ kiểm tra key có tồn tại: ngày 2026-10-05 một lần bootstrap --post-url đã ghi một
    query feed đăng xuất (BarcelonaLoggedOutFeedPaginationQuery) vào đúng key này. Query đó
    không có biến `query`, nên mọi lượt search sau đó âm thầm trả về feed chung thay vì kết quả
    theo từ khoá. Cache mà friendly_name không chứa "search" bị coi là hỏng để bootstrap lại."""
    cache = RedisCache()
    account = cache.get(THREADS_ACTIVE_ACCOUNT_REDIS_KEY) or THREADS_DEFAULT_ACCOUNT_KEY
    session = cache.get(THREADS_CACHE_REDIS_KEY_TMPL.format(account=account))
    if not session:
        return False
    if not is_search_recipe(session):
        logger.warning(
            "threads_session_cache_not_search", account=account, friendly_name=session.get("fb_api_req_friendly_name")
        )
        return False
    return True


async def _ensure_threads_session(
    *, run_id: str | None = None, platform: str | None = "threads", honor_platform_cancel: bool = True
) -> bool:
    """Bản Threads của _ensure_facebook_session: bootstrap search (--query, không bao giờ
    --post-url) khi cache thiếu, hết hạn, hoặc không phải công thức search - xem
    _threads_search_cache_usable."""
    if await asyncio.to_thread(_threads_search_cache_usable):
        return True

    logger.info("threads_session_missing_refreshing_first")
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.threads.auth.bootstrap", "--query", TOKEN_REFRESH_QUERY]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("threads_session_refresh_before_crawl_failed", returncode=returncode)
        return False
    return True


_COMMENTS_CACHE_USABLE_CHECK = {
    "facebook": _facebook_comments_cache_usable,
}


async def _ensure_comments_cache(
    platform: str,
    post_url: str,
    account: str | None = None,
    *,
    run_id: str | None = None,
    honor_platform_cancel: bool = True,
) -> bool:
    """Cùng ý tưởng với _ensure_facebook_session, cho cache query comment của một nền tảng thay
    vì cache tìm kiếm - bootstrap nó từ post_url (một bài thật mà request này đã nêu, để
    bootstrap có thứ để mở và bắt query comment) ở lần đầu cache thiếu/không dùng được (xem
    phép kiểm tra riêng của từng nền tảng trong _COMMENTS_CACHE_USABLE_CHECK).

    Threads không đi đường này mà bootstrap search (xem _ensure_threads_session).

    `account`: chỉ cho facebook, truyền cho bootstrap.py dưới dạng --account. Xem
    _run_comments_spider để biết vì sao điều này quan trọng."""
    if platform == "threads":
        # Không bootstrap --post-url cho Threads: mở permalink nguội luôn ra route đăng xuất, không
        # bắt được query reply nào, và trước đây còn ghi đè công thức search (xem
        # _threads_search_cache_usable). Spider comment chỉ cần cookie, nên bootstrap search là đủ.
        return await _ensure_threads_session(
            run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
        )
    check = _COMMENTS_CACHE_USABLE_CHECK[platform]
    usable = await asyncio.to_thread(check, account)
    if usable:
        return True

    logger.info("comments_cache_missing_bootstrapping", platform=platform, post_url=post_url, account=account)
    args = [PYTHON_BIN, "-m", COMMENTS_BOOTSTRAP_MODULE[platform], "--post-url", post_url]
    if account and platform == "facebook":
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("comments_bootstrap_failed", platform=platform, returncode=returncode)
        return False
    return True


async def _ensure_replies_cache(
    post_url: str,
    account: str | None = None,
    *,
    run_id: str | None = None,
    platform: str = "facebook",
    honor_platform_cancel: bool = True,
) -> bool:
    """Chỉ cho Facebook (Threads/TikTok không có bản tương đương - xem docstring của
    _fetch_replies về việc vì sao Threads không cần và comment TikTok hoàn toàn không nằm
    trong COMMENTS_PLATFORMS_NEEDING_CACHE): cùng ý tưởng với _ensure_comments_cache, cho
    cache query reply riêng mà một comment có reply cần (_facebook_replies_cache_usable).
    Bootstrap với --type replies, vốn cần một post_url có comment cấp một chứa ít nhất một
    comment có reply để bấm mở - chính post_url mà lượt crawl comment được xếp hàng cho trên
    thực tế là đủ (phần lớn bài thật có tương tác đều có ít nhất một reply ở đâu đó), và một
    lần bootstrap không tìm được thì chỉ thất bại gọn gàng (comments_bootstrap_failed) thay
    vì âm thầm không cache gì."""
    if await asyncio.to_thread(_facebook_replies_cache_usable, account):
        return True

    logger.info("replies_cache_missing_bootstrapping", post_url=post_url, account=account)
    args = [
        PYTHON_BIN,
        "-m",
        "social_crawler.spiders.facebook.auth.bootstrap",
        "--post-url",
        post_url,
        "--type",
        "replies",
    ]
    if account:
        args += ["--account", account]
    returncode = await _run_subprocess(
        args, run_id=run_id, platform=platform, honor_platform_cancel=honor_platform_cancel
    )
    if returncode != 0:
        logger.error("replies_bootstrap_failed", returncode=returncode)
        return False
    return True


async def _run_comments_spider(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Xử lý request type="comments" (xem publish_comments_crawl_request của cinemark-api) -
    chạy spider comment của nền tảng đích cho đúng một bài (xem COMMENTS_SPIDER_BY_PLATFORM;
    nền tảng không có trong đó thì hoàn toàn chưa có tính năng comment - xem docstring
    get_comment_mapper phía cinemark-api). Dùng chung crawl_job:<platform> với các lượt crawl
    tìm kiếm của _run_spider, đây là chủ đích, không phải sơ suất - cả hai đi qua cùng vòng
    lặp consumer từng-cái-một theo nền tảng (xem docstring module), nên một lượt crawl
    comment và một lượt crawl tìm kiếm của cùng nền tảng vốn không bao giờ chạy đồng thời
    trên cùng tài khoản/session."""
    platform = request.get("platform", "facebook")
    spider_name = COMMENTS_SPIDER_BY_PLATFORM.get(platform)
    if spider_name is None:
        logger.warning("unsupported_comments_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported comments platform {platform}")

    post_id = request.get("post_id")
    post_url = request.get("post_url")
    if not post_id or not post_url:
        logger.warning("comments_request_missing_fields", request=request)
        raise CrawlJobFailed("comments request missing post_id or post_url")

    # Báo job *trước khi* ensure_*/bootstrap để Dừng huỷ được việc bắt cache comment kéo dài -
    # trước đây crawl_job chỉ được đặt khi spider scrapy bắt đầu, nên lần Dừng đầu tiên trong
    # lúc bootstrap chỉ bật drain mà không kill được gì, và lượt crawl vẫn chạy tiếp.
    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "type": "comments",
                "post_id": post_id,
                "started_at": int(time.time()),
            },
        )

    def _stopped() -> bool:
        return _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain)

    returncode: int | None = None
    try:
        # Ghim một tài khoản cho mọi lời gọi ensure_* bên dưới thay vì để mỗi lời gọi tự đọc/xoay
        # ACTIVE_ACCOUNT_REDIS_KEY - xem docstring của _facebook_replies_cache_usable cho lỗi mà
        # việc này sửa. Bắt đầu bằng một phỏng đoán tốt nhất (tài khoản nào đang active);
        # _ensure_facebook_session có thể bootstrap một tài khoản *khác* nếu phỏng đoán đó hoá ra
        # đã cũ (chưa có dòng tài khoản, hoặc session hết hạn), nên đọc lại ngay sau đó - mọi lời
        # gọi ensure_* tiếp theo nhắm rõ vào đúng tài khoản đó, giờ đã xác nhận còn sống.
        target_account = RedisCache().get(ACTIVE_ACCOUNT_REDIS_KEY) if platform == "facebook" else None
        if platform == "facebook":
            if _stopped():
                logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
                raise CrawlJobSkipped("comments cancelled before start")
            if not await _ensure_facebook_session(
                target_account, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
            ):
                raise CrawlJobFailed("facebook session refresh failed before comments crawl")
            if target_account is not None and not await asyncio.to_thread(_facebook_session_is_cached, target_account):
                # _ensure_facebook_session đã phải chuyển sang tài khoản khác (tài khoản ghim ban đầu chết) -
                # bỏ ghim để đọc lại tài khoản active mới bên dưới.
                target_account = None
            if target_account is None:
                # Chỉ tin một lần đọc mới ở đây khi ta chưa ghim tài khoản cụ thể nào - next_account() của
                # bootstrap.py vừa chọn một tài khoản (trước đó chưa có gì để ghim), nên đây là chỗ duy
                # nhất biết được lựa chọn đó. Nếu target_account đã được đặt, _ensure_facebook_session đã
                # kiểm tra/bootstrap *đúng tài khoản đó* - đọc lại key dùng chung ở đây có thể lấy phải
                # một thay đổi đồng thời không liên quan và âm thầm gỡ ghim, đúng lỗi mà cả cơ chế này
                # sinh ra để ngăn.
                target_account = RedisCache().get(ACTIVE_ACCOUNT_REDIS_KEY)
        if platform in COMMENTS_PLATFORMS_NEEDING_CACHE:
            if _stopped():
                logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
                raise CrawlJobSkipped("comments cancelled before start")
            comments_ok = (
                await _ensure_comments_cache(
                    platform, post_url, target_account, run_id=run_id, honor_platform_cancel=not bypass_drain
                )
                if platform == "facebook"
                else await _ensure_comments_cache(
                    platform, post_url, run_id=run_id, honor_platform_cancel=not bypass_drain
                )
            )
            if not comments_ok:
                raise CrawlJobFailed("comments cache bootstrap failed")
        include_replies = False
        if platform == "facebook":
            # Cố gắng hết mức, KHÔNG phải cổng chặn phần còn lại của job (2026-09-16, sửa ngay trong
            # ngày đưa vào) - đã xác nhận thực tế việc coi nó là bắt buộc làm lượng comment thật lấy
            # được tụt còn khoảng 0,02% trên một lô 100 bài: phần lớn bài không có chuỗi reply mà
            # bootstrap này dễ bám vào (cần một comment có số reply đủ lớn để phân trang - xem
            # docstring của _ensure_replies_cache), nên bắt buộc nó trước khi chạy BẤT KỲ lượt crawl
            # comment nào đã làm huỷ cả job - mất luôn comment cấp một hoàn toàn lấy được của bài đó -
            # thay vì chỉ mất đúng phần thực sự không có. _fetch_replies của comments.py vốn đã tự hạ
            # cấp êm theo từng comment khi thiếu cache này (log replies_pagination_not_bootstrapped
            # rồi chuyển sang comment kế tiếp) - chưa bao giờ cần chặn cả job vì nó.
            #
            # Bỏ qua hẳn trừ khi request chủ động bật reply: mỗi lần bootstrap reply + lượt gọi reply
            # cho từng comment chiếm phần lớn thời gian trên bài nhiều comment, và đường GraphQL reply
            # hiện trả về edges rỗng với phần lớn tài khoản. Tham số spider mặc định là
            # include_replies=false cũng vì lý do đó.
            include_replies = str(request.get("include_replies", "false")).lower() not in (
                "false",
                "0",
                "no",
                "",
            )
            if include_replies and not await _ensure_replies_cache(
                post_url, target_account, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
            ):
                logger.warning(
                    "replies_cache_unavailable_continuing",
                    post_url=post_url,
                    account=target_account,
                    note="top-level comments will still be fetched; replies on any comment will be skipped this run",
                )

        if _stopped():
            logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
            raise CrawlJobSkipped("comments cancelled before start")

        if platform == "tiktok":
            # tiktok_comments nhận video_id/video_url, không phải post_id/post_url - nó không bao giờ
            # đụng tới doc_id/token đã cache như Facebook/Threads, nên cũng không cần dùng chung tên
            # tham số chung của chúng.
            args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"video_id={post_id}", "-a", f"video_url={post_url}"]
        else:
            args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"post_id={post_id}"]
            if platform == "threads":
                # Không bắt buộc; reply qua REST chỉ cần post_id. Giữ lại để dòng log/debug vẫn có
                # permalink mà dashboard đã xếp hàng.
                args += ["-a", f"post_url={post_url}"]
                # text_feed nhận tới khoảng 100 mỗi trang; consumer trước đây không truyền count và thừa
                # hưởng 25 của spider, đốt ngân sách max_pages sớm trên các chuỗi lớn. Vẫn không vượt được
                # trần hiển thị theo xếp hạng của Threads (cursor kết thúc với phần còn lại của badge chưa
                # đọc).
                args += ["-a", f"count={request.get('count', 100)}"]
            if platform == "facebook":
                # Ghim đúng tài khoản mà mọi lời gọi ensure_* ở trên vừa xác minh (hoặc vừa bootstrap)
                # dùng được cho session+comments+replies, thay vì để FacebookCommentsSpider tự quay về đọc
                # ACTIVE_ACCOUNT_REDIS_KEY khi tiến trình scrapy con thực sự bắt đầu ngay sau đó - đã xảy
                # ra thật (2026-09-16), hai lần: lần đầu là thứ khác (một lần chạy bootstrap đồng thời bên
                # ngoài vòng lặp từng-cái-một của consumer này, ví dụ người tự chạy bootstrap.py bằng tay,
                # hoặc cron refresh_token.sh 4 giờ) trỏ lại ACTIVE_ACCOUNT_REDIS_KEY giữa phép kiểm tra
                # này và lúc tiến trình con đọc nó; lần nữa là việc xoay next_account() của từng lời gọi
                # ensure_* chọn một tài khoản *khác* với lần trước, nên tới cuối thì tài khoản "active"
                # chưa bao giờ thực sự được xác minh đủ cả ba cache cùng lúc. Dùng thẳng target_account
                # (không đọc mới từ Redis) tránh được cả hai.
                if target_account:
                    args += ["-a", f"account={target_account}"]
                # Trang phân trang Comet dày nhất (trình duyệt dùng -1) + bỏ qua đường reply hỏng/chậm trừ
                # khi hàng đợi chủ động bật.
                args += ["-a", f"count={request.get('count', -1)}"]
                args += ["-a", f"include_replies={'true' if include_replies else 'false'}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]

        logger.info("comments_crawl_started", platform=platform, post_id=post_id, run_id=run_id)
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode is None:
        raise CrawlJobSkipped("comments cancelled before start")
    _raise_for_returncode(returncode, f"{platform} comments crawl post_id={post_id}")
    logger.info("comments_crawl_finished", platform=platform, post_id=post_id)


async def _run_channel_videos_spider(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Xử lý type="channel_videos" (chỉ TikTok) - scrapy tiktok_channel_videos cho một
    @username. Dùng chung crawl_job:tiktok với tìm kiếm hashtag / comments (cùng vòng lặp
    consumer từng-cái-một)."""
    platform = request.get("platform") or "tiktok"
    if platform != "tiktok":
        logger.warning("unsupported_channel_videos_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"channel_videos only supported on tiktok, got {platform}")

    username = str(request.get("username") or "").lstrip("@").strip()
    if not username:
        logger.warning("channel_videos_missing_username", request=request)
        raise CrawlJobFailed("channel_videos request missing username")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "type": "channel_videos",
                "username": username,
                "started_at": int(time.time()),
            },
        )

    def _stopped() -> bool:
        return _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain)

    if _stopped():
        logger.info("channel_videos_cancelled_before_start", username=username)
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))
        raise CrawlJobSkipped("channel_videos cancelled before start")

    args = [SCRAPY_BIN, "crawl", TIKTOK_CHANNEL_VIDEOS_SPIDER, "-a", f"username={username}"]
    if request.get("keyword_id"):
        args += ["-a", f"keyword_id={request['keyword_id']}"]
    if request.get("max_pages"):
        args += ["-a", f"max_pages={request['max_pages']}"]

    logger.info("channel_videos_started", username=username, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    _raise_for_returncode(returncode, f"tiktok channel_videos username={username}")
    logger.info("channel_videos_finished", username=username)


async def _run_spider(request: dict[str, Any]) -> None:
    platform = request.get("platform")

    # Một lần bấm Dừng bật bfs_drain:<platform> trong Redis (xem crawl_jobs.request_stop của
    # cinemark-api) chính là để các lượt crawl tìm ra qua BFS đã xếp hàng sau thứ nó huỷ không
    # cứ thế chạy tiếp từng cái một - chỉ request có bfs_depth mới bị drain (bfs_depth không
    # bao giờ được đặt trên request thật kích hoạt từ dashboard/cron), nên một lượt crawl xếp
    # hàng bằng tay cho từ khoá khác vẫn chạy bình thường kể cả khi drain đang bật.
    if request.get("bfs_depth") is not None and RedisCache().exists(f"bfs_drain:{platform}"):
        logger.info("bfs_request_skipped_drain", platform=platform, keyword=request.get("keyword"))
        raise CrawlJobSkipped("bfs drain")

    keyword = request.get("keyword")
    if not keyword:
        logger.warning("crawl_request_missing_keyword", request=request)
        raise CrawlJobFailed("crawl request missing keyword")

    if platform not in ("facebook", "threads", "tiktok"):
        logger.warning("unsupported_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported platform {platform}")

    if platform == "tiktok" and not str(keyword).startswith("#"):
        logger.info(
            "crawl_request_skipped_tiktok_text",
            keyword=keyword,
            keyword_id=request.get("keyword_id"),
        )
        raise CrawlJobSkipped("tiktok is hashtag-only")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "keyword": keyword,
                "keyword_id": request.get("keyword_id"),
                "started_at": int(time.time()),
            },
        )

    if platform == "tiktok":
        args = [SCRAPY_BIN, "crawl", TIKTOK_HASHTAG_SPIDER, "-a", f"hashtag={keyword.lstrip('#')}"]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
        if request.get("bfs_depth"):
            args += ["-a", f"bfs_depth={request['bfs_depth']}"]
    else:
        args = [
            SCRAPY_BIN,
            "crawl",
            SPIDER_BY_PLATFORM[platform],
            "-a",
            f"query={keyword}",
            "-a",
            "include_entities=false",
        ]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
        # Khoảng ngày chỉ có ở Facebook (Threads không có bộ lọc ngày đăng).
        if platform == "facebook":
            if request.get("start_date"):
                args += ["-a", f"start_date={request['start_date']}"]
            if request.get("end_date"):
                args += ["-a", f"end_date={request['end_date']}"]
            args += ["-a", f"sweep_days={_sweep_days_for(request)}"]
        elif request.get("start_date") or request.get("end_date"):
            logger.info(
                "crawl_request_dates_dropped",
                platform=platform,
                keyword=keyword,
                keyword_id=request.get("keyword_id"),
                start_date=request.get("start_date"),
                end_date=request.get("end_date"),
            )

    # Được publish_crawl_request của cinemark-api đặt cho mọi lượt chạy kích hoạt từ dashboard,
    # và cho các bước nhảy BFS đã được người vận hành duyệt (chip hashtag liên quan). Thứ gì
    # đang chạy cho nền tảng này là thứ nút Dừng trên dashboard huỷ (xem crawl_jobs.py).
    logger.info(
        "crawl_request_started", platform=platform, keyword=keyword, keyword_id=request.get("keyword_id"), run_id=run_id
    )
    try:
        if platform == "facebook" and not await _ensure_facebook_session(run_id=run_id, platform=platform):
            raise CrawlJobFailed("facebook session refresh failed before search crawl")
        if platform == "threads" and not await _ensure_threads_session(run_id=run_id, platform=platform):
            raise CrawlJobFailed("threads session refresh failed before search crawl")
        returncode = await _run_subprocess(args, run_id=run_id, platform=platform)
        _raise_for_returncode(returncode, f"{platform} crawl keyword={keyword}")
        logger.info("crawl_request_finished", platform=platform, keyword=keyword)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))


async def _refresh_tiktok_identity(request: dict[str, Any]) -> None:
    """Bản tương đương của _refresh_token bên dưới cho TikTok, nhưng có dạng khác: không có
    cache token GraphQL nào để bắt lại. Restore / bước tiếp theo sau import cookie dùng lại
    cùng phần theo dõi job/huỷ, rồi chạy tiktok/auth/bootstrap.py trên một dòng
    platform_accounts đã ghim (device_id/odinId từ cookie đã lưu)."""
    account_id = request.get("account_id")
    account_key = request.get("account_key")
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.tiktok.auth.bootstrap"]
    if account_id is not None:
        args += ["--account-id", str(account_id)]
    elif account_key:
        args += ["--account", str(account_key)]
    else:
        logger.warning("tiktok_refresh_missing_account", request=request)
        raise CrawlJobFailed("tiktok refresh missing account_id/account_key")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform="tiktok")
    if run_id:
        args += ["--run-id", str(run_id)]
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info(
        "token_refresh_started", platform="tiktok", account_id=account_id, account_key=account_key, run_id=run_id
    )
    try:
        returncode = await _run_subprocess(args, run_id=run_id, platform="tiktok", honor_platform_cancel=False)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode != 0:
        logger.error(
            "token_refresh_failed",
            platform="tiktok",
            account_id=account_id,
            account_key=account_key,
            returncode=returncode,
            run_id=run_id,
        )
        _mark_refresh_result(run_id, ok=False)
        _raise_for_returncode(returncode, "tiktok token refresh")
    else:
        logger.info(
            "token_refresh_finished",
            platform="tiktok",
            account_id=account_id,
            account_key=account_key,
            run_id=run_id,
        )
        _mark_refresh_result(run_id, ok=True)


async def _refresh_token(request: dict[str, Any]) -> None:
    """Cùng lệnh mà cron 4 giờ của scripts/refresh_token.sh vẫn chạy cho Facebook - chỉ là kích
    hoạt khi cần thay vì chờ lượt kế tiếp. Cũng xử lý Threads theo cùng cách, vì bootstrap.py
    của nó giống luồng cache token của Facebook từng trường một (xem
    spiders/threads/auth/bootstrap.py). platform/started/finished/failed được log rõ ràng
    (không chỉ suy ra từ tên module) vì refresh_tracker của cinemark-api đọc dần đúng file
    log này và khớp theo "token_refresh_{started,finished,failed} ... platform=<x>" để biết
    khi nào một lần refresh kích hoạt từ dashboard đã xong - xem
    cinemark-api/app/services/refresh_tracker.py."""
    platform = request.get("platform", "facebook")
    if platform == "tiktok":
        await _refresh_tiktok_identity(request)
        return

    module = TOKEN_REFRESH_BOOTSTRAP_MODULE.get(platform)
    if module is None:
        logger.warning("unsupported_refresh_token_platform", platform=platform)
        raise CrawlJobFailed(f"unsupported refresh_token platform {platform}")

    run_id = request.get("run_id")
    # --run-id (khi có) được bind vào context structlog của chính tiến trình con (xem __main__
    # trong auth/bootstrap.py của facebook/threads), nên mọi dòng nó log - không chỉ
    # started/finished/failed của hàm này - đều mang run_id=<x>. refresh_tracker của
    # cinemark-api lọc các dòng đọc được theo đúng dấu đó (xem docstring module đó để biết vì
    # sao khớp chuỗi con platform= trần không đủ chính xác khi crawl_request_consumer.py bắt
    # đầu chạy song song mỗi nền tảng một task thay vì một vòng lặp tuần tự toàn cục).
    args = [PYTHON_BIN, "-m", module, "--query", TOKEN_REFRESH_QUERY]
    account_key = request.get("account_key")
    if account_key:
        # Ghim vào một dòng (restore trên dashboard / bước tiếp theo sau import cookie). Không có
        # cái này, việc xoay vòng bỏ qua tài khoản bị checkpoint và bắt lại token cho tài khoản nào
        # còn khoẻ thay vì tài khoản người vận hành vừa chọn.
        args += ["--account", str(account_key)]
    if run_id:
        args += ["--run-id", str(run_id)]

    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info("token_refresh_started", platform=platform, run_id=run_id)
    try:
        returncode = await _run_subprocess(args, run_id=run_id, platform=platform, honor_platform_cancel=False)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode != 0:
        logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
        _mark_refresh_result(run_id, ok=False)
        _raise_for_returncode(returncode, f"{platform} token refresh")
    else:
        logger.info("token_refresh_finished", platform=platform, run_id=run_id)
        _mark_refresh_result(run_id, ok=True)


async def _import_cookies(request: dict[str, Any]) -> None:
    """Import cookie kích hoạt từ dashboard (xem POST /<platform>/import-cookies của
    cinemark-api) - chạy đúng luồng `bootstrap.py --cookies-file ... --account ...` mà
    người dùng lẽ ra phải tự gõ trong terminal (xem import_cookies trong auth/cookies.py của
    facebook/threads), chỉ là kích hoạt từ form "Nhập cookie" của trang Settings.

    Chỉ tự động hoá bước "đưa cookie đã xuất vào Redis" - bản thân việc đăng nhập/2FA đã xảy
    ra trong một trình duyệt thật do người điều khiển (tài khoản không có cookie thì tự đăng
    nhập qua proxy đã ghim - xem auth/bootstrap.py của facebook/threads).

    Nối thẳng sang một lần refresh token bình thường khi import thành công - --cookies-file
    chỉ gọi import_cookies() (lưu session vào Redis), không bao giờ đi tiếp để bắt các token
    phát lại GraphQL mà một lượt crawl thực sự cần (xem phần điều phối __main__ của
    bootstrap.py). Không có bước thứ hai này, import cookie sẽ để tài khoản trông như đã xong
    trên dashboard nhưng vẫn lỗi ở ngay lượt crawl kế tiếp - cần một lần kích hoạt tay thứ
    hai mà không ai biết để chờ. Cố ý không còn nút/route "refresh login" riêng (xem lịch sử
    token_refresh.py của cinemark-api) - một session mới chỉ cần làm mới khi đã tồn tại, và
    đây là nơi duy nhất tạo ra nó.

    Dùng lại token_refresh_{started,finished,failed} - không phải tên event mới - để
    refresh_tracker.py của cinemark-api và RefreshLogPanel/TokenStatusBadge có sẵn trên
    dashboard hiển thị tiến độ lượt chạy này theo đúng cách đã làm với lần refresh thường,
    không cần nối thêm gì ở frontend."""
    platform = request.get("platform", "facebook")
    account_key = request.get("account_key")
    cookies_json = request.get("cookies")
    run_id = request.get("run_id")
    if not account_key or not cookies_json:
        logger.warning("cookie_import_missing_fields", platform=platform, run_id=run_id)
        raise CrawlJobFailed("cookie import missing account_key or cookies")

    module = TOKEN_REFRESH_BOOTSTRAP_MODULE.get(platform)
    if module is None:
        logger.warning("unsupported_refresh_token_platform", platform=platform, run_id=run_id)
        raise CrawlJobFailed(f"unsupported cookie_import platform {platform}")

    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info("token_refresh_started", platform=platform, run_id=run_id, source="cookie_import")
    try:
        # Bước 1: đưa cookie do người xuất vào Redis. Dùng file tạm, không truyền --cookies-json
        # qua argv: đường --cookies-file của bootstrap.py đã có sẵn và đã được kiểm chứng (đây
        # đúng là thứ người dùng vẫn tự chạy bằng tay hiện nay) - dùng lại nó ở đây nghĩa là không
        # phải sửa CLI/parse gì trong bootstrap.py, chỉ thêm một chỗ gọi mới.
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        try:
            tmp.write(cookies_json)
            tmp.close()
            import_args = [PYTHON_BIN, "-m", module, "--cookies-file", tmp.name, "--account", account_key]
            if run_id:
                import_args += ["--run-id", str(run_id)]
            returncode = await _run_subprocess(
                import_args, run_id=run_id, platform=platform, honor_platform_cancel=False
            )
        finally:
            Path(tmp.name).unlink(missing_ok=True)

        if returncode != 0:
            logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
            _mark_refresh_result(run_id, ok=False)
            _raise_for_returncode(returncode, f"{platform} cookie import")

        # Import kiểu Copy-as-cURL của TikTok đã ghi device_id/odin_id/cookie và đổi account_id
        # thành device_id. Chạy bootstrap lần hai với --account <ghim ban đầu> sẽ không tìm thấy
        # dòng. Danh tính đã đầy đủ.
        if platform == "tiktok":
            logger.info("token_refresh_finished", platform=platform, run_id=run_id, source="cookie_import")
            _mark_refresh_result(run_id, ok=True)
            return

        # Facebook/Threads bắt lại token phát lại GraphQL.
        refresh_args = [PYTHON_BIN, "-m", module, "--query", TOKEN_REFRESH_QUERY, "--account", str(account_key)]
        if run_id:
            refresh_args += ["--run-id", str(run_id)]
        returncode = await _run_subprocess(refresh_args, run_id=run_id, platform=platform, honor_platform_cancel=False)
        if returncode != 0:
            logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
            _mark_refresh_result(run_id, ok=False)
            _raise_for_returncode(returncode, f"{platform} cookie import token refresh")
        else:
            logger.info("token_refresh_finished", platform=platform, run_id=run_id)
            _mark_refresh_result(run_id, ok=True)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))


async def _nurture_accounts(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Chạy nurture_accounts.py cho một nền tảng (facebook, threads hoặc tiktok). Settings trên
    dashboard xếp hàng các lượt này dưới dạng type=nurture trên crawl_requests - cùng consumer
    từng-cái-một như crawl, nên một lượt làm ấm không bao giờ chồng lên một lượt tìm kiếm
    của cùng tài khoản/session."""
    platform = request.get("platform")
    if platform not in ("facebook", "threads", "tiktok"):
        logger.warning("nurture_unsupported_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported nurture platform {platform}")

    args = [PYTHON_BIN, "-m", "social_crawler.nurture_accounts", "--platform", platform]
    account = request.get("account")
    if account:
        # Một tài khoản được chỉ định: vẫn tôn trọng hạn mức hằng ngày, nhưng không chờ vài phút
        # trước khi bắt đầu - người vận hành vừa bấm vào đúng dòng đó.
        args += ["--account", str(account), "--limit", "1", "--gap-min", "30", "--gap-max", "90"]
        args += ["--start-delay-min", "15", "--start-delay-max", "75"]
    else:
        # Làm ấm cả pool: tối đa hai tài khoản mỗi nền tảng, bỏ qua tài khoản đã làm ấm hôm nay,
        # chờ một chút trước lượt lướt đầu tiên và vài phút giữa các tài khoản để một cú bấm không
        # thành một đợt dồn dập.
        args += ["--limit", "2", "--gap-min", "180", "--gap-max", "540"]
        args += ["--start-delay-min", "45", "--start-delay-max", "240"]
    args.append("--like" if request.get("like", True) else "--no-like")

    count = request.get("visits", 3)
    try:
        count_n = max(0, min(8, int(count)))
    except TypeError, ValueError:
        count_n = 3
    if platform == "tiktok":
        # Đường tiktok của nurture_accounts.py không có khái niệm --comment (feed của nó là cuộn
        # liên tục, không phải các bài riêng lẻ có ô soạn thảo - xem docstring của module đó) và
        # dùng lại trường "visits" của request mà docstring publish_nurture_request của
        # cinemark-api đã ghi là "số trang hashtag cần vào" cho nền tảng này - --hashtags 0 không
        # hợp lệ ở đó (ít nhất vào 1 trang), khác với --visits 0 của facebook/threads (chỉ
        # comment/lướt, không mở bài nào).
        args += ["--hashtags", str(max(1, count_n))]
    else:
        # Không có trường comment = tắt (xem --comment trong nurture_accounts.py).
        args.append("--comment" if request.get("comment", False) else "--no-comment")
        args += ["--visits", str(count_n)]

    run_id = request.get("run_id")
    if run_id:
        args += ["--run-id", str(run_id)]
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "nurture", "account": account, "started_at": int(time.time())})

    logger.info("nurture_started", platform=platform, account=account, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    _raise_for_returncode(returncode, f"{platform} nurture account={account}")
    logger.info("nurture_finished", platform=platform, account=account)


async def _check_cookies(request: dict[str, Any]) -> None:
    """Kiểm tra định kỳ cookie còn đăng nhập không (scripts/check_facebook_cookies.py - không đăng nhập, chỉ mở
    facebook.com bằng cookie sẵn có qua proxy đã ghim). Tài khoản chết được ghi last_check_status='dead' để auto-login
    nạp cookie mới. Bộ lập lịch cinemark-api xếp request này (type=cookie_check) - cùng hàng đợi từng-cái-một với
    crawl nên không bao giờ mở cùng session song song với một lượt crawl."""
    platform = request.get("platform") or "facebook"
    if platform != "facebook":
        raise CrawlJobFailed(f"cookie_check only supports facebook, got {platform}")
    args = [PYTHON_BIN, "-m", "scripts.check_facebook_cookies"]
    stale_hours = request.get("stale_hours")
    if stale_hours:
        args += ["--stale-hours", str(stale_hours)]
    logger.info("cookie_check_started", platform=platform, stale_hours=stale_hours)
    returncode = await _run_subprocess(args, run_id=request.get("run_id"), platform=platform)
    _raise_for_returncode(returncode, f"{platform} cookie check")
    logger.info("cookie_check_finished", platform=platform)


async def _handle_request(request: dict[str, Any]) -> bool:
    """Trả về True khi request bị bỏ qua (Dừng / drain), để consumer lấy message Kafka kế tiếp
    mà không cần khoảng nghỉ giữa các request."""
    platform = request.get("platform") or ""
    kind = request.get("type")
    # bypass_drain đánh dấu message mà cinemark-api gắn là lần kích hoạt có chủ đích riêng của
    # nó (comments/nurture/channel_videos), không phải phần tiếp tục của hàng tồn hàng loạt mà
    # lần Dừng muốn chặn - xem docstring publish_action_request phía cinemark-api. Cũng được
    # truyền xuống cả ba handler đó bên dưới (không chỉ cổng vào này): mỗi handler tự kiểm tra
    # định kỳ lại cùng các tín hiệu drain/huỷ cho cả nền tảng, trước/trong tiến trình con của
    # mình, và phải bỏ qua chúng theo cùng cách, nếu không một tín hiệu cũ từ lần Dừng trước sẽ
    # giết chính job mà bypass_drain muốn cho qua (đã xảy ra thật 2026-09-21 - một lượt nurture
    # bị SIGTERM bởi crawl_job_cancel_platform còn sót chỉ vài giây sau khi bắt đầu).
    bypass_drain = bool(request.get("bypass_drain"))

    # Một lần Dừng chính xác theo từng job (POST /<platform>/jobs/{run_id}/stop của
    # cinemark-api - xem crawl_jobs.cancel_job) nhắm đúng run_id NÀY, được bấm khi nó còn đang
    # xếp hàng chứ chưa chạy. Luôn được tôn trọng, dù có bypass_drain hay không: khác với phép
    # kiểm tra drain cho cả nền tảng bên dưới (mà một lần kích hoạt có chủ đích phải vượt qua
    # được), tín hiệu này chỉ tồn tại vì người dùng đã bấm Dừng đúng job này.
    run_id = request.get("run_id")
    if run_id and _cancel_requested(run_id=run_id):
        logger.info("request_skipped_job_cancel", platform=platform, run_id=run_id, type=kind or "search")
        finish_task(request, "skipped")
        return True

    # Dừng bỏ mọi thứ đã xếp hàng, kể cả các lần kích hoạt có chủ đích - bypass_drain chỉ cho
    # qua những gì được bấm *sau* lần Dừng đó. Không có cái này, các message nurture/comments
    # xếp hàng trước khi Dừng cứ lần lượt bắt đầu trong khi dashboard không hiện gì.
    if bypass_drain and platform and _published_before_stop(request, platform):
        logger.info("request_skipped_stopped", platform=platform, run_id=run_id, type=kind or "search")
        finish_task(request, "skipped")
        return True

    if (
        kind not in ("refresh_token", "cookie_import", "cookie_check")
        and not bypass_drain
        and platform
        and is_platform_draining(platform)
    ):
        logger.info("request_skipped_drain", platform=platform, type=kind or "search", post_id=request.get("post_id"))
        finish_task(request, "skipped")
        return True

    start_task(request)
    try:
        if request.get("type") == "refresh_token":
            await _refresh_token(request)
        elif request.get("type") == "cookie_import":
            await _import_cookies(request)
        elif request.get("type") == "comments":
            await _run_comments_spider(request, bypass_drain=bypass_drain)
        elif request.get("type") == "channel_videos":
            await _run_channel_videos_spider(request, bypass_drain=bypass_drain)
        elif request.get("type") == "nurture":
            await _nurture_accounts(request, bypass_drain=bypass_drain)
        elif request.get("type") == "cookie_check":
            await _check_cookies(request)
        else:
            await _run_spider(request)
    except CrawlJobSkipped:
        finish_task(request, "skipped")
        return True
    except CrawlJobFailed as exc:
        finish_task(request, "failed", error=str(exc))
        return False
    except Exception:
        finish_task(request, "failed")
        raise
    finish_task(request, "done")
    return False


def _request_platform(request: dict[str, Any]) -> str | None:
    """Vòng lặp nền tảng nào nên nhận message này. Chỉ request refresh_token mới nhận mặc định
    "facebook" cũ mà chính _refresh_token đã dùng, cho message xếp hàng từ trước khi hỗ trợ
    nhiều nền tảng - một crawl request thường được kỳ vọng luôn đặt "platform" rõ ràng (xem
    _run_spider, vốn không có mặc định như vậy). Đặt mặc định cho *mọi* loại message ở đây
    trước đây khiến một crawl request không có platform bị vòng lặp facebook nhận rồi lại bị
    từ chối ở đó là unsupported_platform; trả None cho trường hợp đó nghĩa là không vòng lặp
    nào nhận, nên nó rơi xuống dòng log nền tảng không nhận ra bên dưới, đúng như _run_spider
    từng tự tạo ra."""
    if request.get("type") == "refresh_token":
        return request.get("platform", "facebook")
    return request.get("platform")


async def _requeue_after_proxy_exhaustion(
    requeue_publisher: KafkaPublisher, request: dict[str, Any], *, platform: str
) -> None:
    """Publish lại `request` lên crawl_requests với bộ đếm "_proxy_exhausted_retries" của nó
    tăng thêm - xem comment của setting exhausted_max_requeues ở trên để biết vì sao riêng
    loại lỗi này (và chỉ loại này) được thử lại thật thay vì bị log rồi mất như mọi exception
    khác mà _handle_request có thể raise. Bỏ cuộc (log rõ ràng, không publish lại) khi bộ đếm
    chạm trần - một nền tảng có pool proxy chết hẳn, không chỉ đang cooldown, không được lặp
    mãi."""
    retries = request.get("_proxy_exhausted_retries", 0)
    max_requeues = int((await asyncio.to_thread(get_proxy_settings))["exhausted_max_requeues"])
    if retries >= max_requeues:
        logger.error(
            "proxy_exhausted_requeue_gave_up",
            telegram=True,
            platform=platform,
            retries=retries,
            request=request,
        )
        return
    requeued = {**request, "_proxy_exhausted_retries": retries + 1}
    await requeue_publisher.publish(
        topic=CRAWL_REQUESTS_TOPIC,
        key=request.get("run_id") or platform,
        value=requeued,
    )
    logger.info("proxy_exhausted_requeued", platform=platform, retries=retries + 1, request=request)


async def _run_platform_consumer_once(platform: str, group_id: str) -> bool:
    """Một lần chạy vòng lặp consumer của một nền tảng - xem docstring module để biết vì sao có
    mỗi nền tảng một vòng thay vì một vòng dùng chung. Instance nào cũng subscribe cùng topic
    và thấy mọi message; thứ gì không thuộc nền tảng này được commit qua ngay (không crawl,
    không nghỉ) để thông lượng của vòng lặp này không bao giờ bị ảnh hưởng bởi lưu lượng của
    các nền tảng khác.

    Trả về True khi _run_platform_consumer nên ngừng thử lại (KeyboardInterrupt có chủ đích;
    CancelledError thì được lan ra, không xử lý, vì đó là chỗ gọi bên ngoài - run(), hoặc lúc
    tắt - đang chủ động dừng task này), False khi kết thúc vì một KafkaError đáng thử lại với
    consumer mới (xem _run_platform_consumer)."""
    consumer = AIOKafkaConsumer(
        CRAWL_REQUESTS_TOPIC,
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        # "earliest": một request xếp hàng lúc consumer này tình cờ đang sập vẫn nên chạy khi nó
        # lên lại, không bị âm thầm bỏ - crawl request đủ hiếm (bấm nút, một lô hằng ngày) nên xử
        # lý một hàng tồn ngắn khi restart không bao giờ là vấn đề.
        auto_offset_reset="earliest",
        # Chỉ commit sau khi _handle_request trả về (xem lời gọi consumer.commit() rõ ràng bên
        # dưới), không theo bộ hẹn giờ 5s mặc định của aiokafka. Một lượt crawl có thể chạy nhiều
        # phút - bộ hẹn giờ mặc định sẽ commit offset của message gần như ngay sau khi nhận, rất
        # lâu trước khi lượt crawl nó kích hoạt thực sự xong. Nếu tiến trình chết giữa lúc crawl,
        # Kafka sẽ không bao giờ giao lại request đó - nó mất luôn, không còn dấu vết gì ngoài một
        # dòng DB "running" mồ côi.
        enable_auto_commit=False,
        # Vòng lặp này chờ trọn một lượt crawl (tới sweep_days=60 theo mặc định, xem
        # _sweep_days_for) rồi mới gọi lại consumer lấy message kế tiếp - max_poll_interval_ms mặc
        # định 5 phút của Kafka không đủ chút nào; vượt quá là broker âm thầm thu hồi tư cách thành
        # viên group giữa lúc crawl. 1 giờ phủ được mọi lượt quét đơn lẻ thực tế. Chỉ các lượt crawl
        # của chính nền tảng này mới tính vào đó - lượt quét dài của nền tảng khác chạy trên
        # consumer/vòng lặp riêng.
        max_poll_interval_ms=3_600_000,
    )
    # Số lần liên tiếp dính pool.ProxyPoolExhaustedError cho vòng lặp nền tảng NÀY - xem
    # PROXY_EXHAUSTED_BACKOFF_* ở trên để biết vì sao cần khoảng nghỉ tăng dần riêng thay vì
    # lao thẳng vào message kế tiếp. Cố ý chỉ trong tiến trình (không Redis): nó chỉ cần sống
    # trong một vòng lặp đang chạy, và về 0 ngay khi có bất cứ thứ gì khác (thành công hoặc lỗi
    # khác) đi qua, y như sức khoẻ tài khoản/proxy mà nó theo dõi vốn tự reset khi thành công.
    proxy_exhausted_streak = 0
    # Chỉ dùng để publish lại một request bị cạn proxy lên crawl_requests (xem
    # _requeue_after_proxy_exhaustion) - một producer riêng, tách khỏi KafkaPublisher của bất
    # kỳ spider nào, vì vòng đời của nó gắn với chính vòng lặp consumer, không với một lượt
    # crawl.
    requeue_publisher = KafkaPublisher()
    try:
        await consumer.start()
        await requeue_publisher.start()
        logger.info("crawl_request_consumer_started", topic=CRAWL_REQUESTS_TOPIC, platform=platform, group=group_id)
        async for message in consumer:
            request = message.value
            request_platform = _request_platform(request)
            if request_platform != platform:
                # Vòng lặp nào cũng thấy mọi message trên topic dùng chung, nên một giá trị platform không
                # khớp vòng nào sẽ hoàn toàn không được log (cả ba vòng đều âm thầm kết luận "không phải
                # của mình"). Chỉ vòng facebook - chọn tuỳ ý, vòng nào cũng được - báo nó, để một nền tảng
                # không nhận ra/gõ sai sinh ra đúng một cảnh báo thay vì ba, khôi phục khả năng quan sát
                # mà _run_spider/_refresh_token từng tự cung cấp trước khi file này có các vòng lặp để
                # định tuyến.
                if platform == "facebook" and request_platform not in PLATFORM_CONSUMER_GROUPS:
                    logger.warning("unsupported_platform", platform=request_platform, request=request)
                await consumer.commit()
                continue
            skipped = False
            proxy_exhausted = False
            try:
                skipped = await _handle_request(request)
            except Exception as exc:
                # Một request lỗi không được giết cả consumer - log rồi chuyển sang cái kế tiếp.
                logger.error("crawl_request_error", platform=platform, error=str(exc), request=request)
                # NetworkError/TikTokNetworkError riêng của mọi nền tảng đều bọc
                # pool.ProxyPoolExhaustedError mà nó được raise từ đó (xem ví dụ
                # `raise ... from exc` trong comet_graphql_client.py/tiktok/client.py) - kiểm tra
                # __cause__ ở đây giúp phần này không phụ thuộc nền tảng, vòng lặp điều phối chung này
                # không cần import exception riêng của từng nền tảng để nhận ra nó.
                proxy_exhausted = isinstance(exc, pool.ProxyPoolExhaustedError) or isinstance(
                    exc.__cause__, pool.ProxyPoolExhaustedError
                )
            # Commit dù _handle_request thành công hay đã bị log rồi bỏ qua ở trên - dù thế nào message
            # này cũng đã xong, không được giao lại ở lần restart sau. Lỗi cạn proxy là ngoại lệ duy
            # nhất: nó được một cơ hội thử lại thật (xem _requeue_after_proxy_exhaustion) - publish lại
            # *trước* lần commit này, không phải sau, để crash ở giữa tệ nhất chỉ để lại một bản trùng
            # vô hại (cả bản gốc, chưa commit và sắp được giao lại khi restart, lẫn bản mới xếp hàng
            # lại) thay vì âm thầm mất request nếu crash rơi vào chiều ngược lại.
            if proxy_exhausted:
                proxy_exhausted_streak += 1
                await _requeue_after_proxy_exhaustion(requeue_publisher, request, platform=platform)
                # to_thread: cache miss sẽ đọc DB đồng bộ.
                proxy_settings = await asyncio.to_thread(get_proxy_settings)
                backoff = min(
                    float(proxy_settings["exhausted_backoff_base_seconds"])
                    * (float(proxy_settings["exhausted_backoff_growth_factor"]) ** (proxy_exhausted_streak - 1)),
                    float(proxy_settings["exhausted_backoff_max_seconds"]),
                )
                logger.warning(
                    "platform_proxy_exhausted_pausing",
                    platform=platform,
                    consecutive_hits=proxy_exhausted_streak,
                    backoff_seconds=round(backoff, 1),
                )
                await consumer.commit()
                await _sleep_interruptible(platform, backoff)
                continue
            await consumer.commit()
            proxy_exhausted_streak = 0
            if skipped or is_platform_draining(platform):
                continue
            if request.get("type") == "comments" and platform != "tiktok":
                # Comment Facebook/Threads chỉ là curl_cffi thường - bỏ khoảng nghỉ giữa các request để một
                # lô 100 bài không bị các lần chờ rảnh rỗi chiếm phần lớn thời gian. Comment TikTok mở một
                # trình duyệt Patchright thật cho mỗi job (xem tiktok/features/comments/comments.py), nên
                # chúng rơi xuống khoảng nghỉ bên dưới như các lượt crawl tìm kiếm.
                continue
            # Xem INTER_REQUEST_PAUSE_MIN/MAX_SECONDS ở trên - một khoảng nghỉ trước khi lấy thứ kế
            # tiếp trong hàng đợi *của nền tảng này*, không phải trước request đầu tiên của một lô mới
            # (chưa có gì để giãn cách).
            pause = random.uniform(INTER_REQUEST_PAUSE_MIN_SECONDS, INTER_REQUEST_PAUSE_MAX_SECONDS)
            logger.info("inter_request_pause", platform=platform, seconds=round(pause, 1))
            await _sleep_interruptible(platform, pause)
    except asyncio.CancelledError:
        logger.info("platform_consumer_cancelled", platform=platform)
        raise
    except KeyboardInterrupt:
        logger.info("platform_consumer_interrupted", platform=platform)
        return True
    except KafkaError as exc:
        logger.error("kafka_error", platform=platform, error=str(exc))
        return False
    finally:
        await consumer.stop()
        await requeue_publisher.stop()
    return True


async def _run_platform_consumer(platform: str, group_id: str) -> None:
    """Khởi động lại _run_platform_consumer_once với consumer mới mỗi khi nó kết thúc vì
    KafkaError, thay vì để một trục trặc Kafka tạm thời của một nền tảng hạ luôn vòng lặp của
    hai nền tảng kia qua phản ứng cuối cùng nặng hơn của run() (xem comment của các hằng
    _CONSUMER_RESTART_*). Chỉ trả về - trao quyền lại cho run(), nơi áp phản ứng nặng hơn đó -
    sau một lần dừng có chủ đích (True) hoặc sau _CONSUMER_RESTART_MAX_ATTEMPTS lần khởi
    động lại trong _CONSUMER_RESTART_WINDOW_SECONDS mà không có trọn một khoảng chạy sạch ở
    giữa."""
    restart_times: list[float] = []
    while True:
        stop_cleanly = await _run_platform_consumer_once(platform, group_id)
        if stop_cleanly:
            return
        now = time.monotonic()
        restart_times = [t for t in restart_times if now - t < _CONSUMER_RESTART_WINDOW_SECONDS]
        restart_times.append(now)
        if len(restart_times) > _CONSUMER_RESTART_MAX_ATTEMPTS:
            logger.error(
                "platform_consumer_restart_giving_up",
                platform=platform,
                attempts=len(restart_times),
                window_seconds=_CONSUMER_RESTART_WINDOW_SECONDS,
            )
            return
        backoff = min(
            _CONSUMER_RESTART_BACKOFF_BASE_SECONDS
            * (_CONSUMER_RESTART_BACKOFF_GROWTH_FACTOR ** (len(restart_times) - 1)),
            _CONSUMER_RESTART_BACKOFF_MAX_SECONDS,
        )
        logger.warning(
            "platform_consumer_restarting",
            platform=platform,
            attempt=len(restart_times),
            backoff_seconds=round(backoff, 1),
        )
        await asyncio.sleep(backoff)


async def run() -> None:
    """Chạy song song vòng lặp consumer của mọi nền tảng trong cùng một tiến trình này - xem
    docstring module. _run_platform_consumer vốn đã tự thử lại KafkaError với consumer mới
    (rebalance/commit thất bại/broker chập chờn - xem docstring của nó), nên khi một task ở
    đây thực sự kết thúc thì đó hoặc là dừng có chủ đích, hoặc là một nền tảng đã dùng hết
    ngân sách thử lại với một broker không truy cập được một thời gian - không phải một trục
    trặc tạm thời đơn lẻ. Trước đây dùng asyncio.gather(..., return_exceptions=True), vốn
    chỉ log chuyện đó và để *hai* vòng kia chạy mãi: crawl của nền tảng đã chết âm thầm dừng
    vĩnh viễn trong khi tiến trình vẫn "đang chạy", và không có gì báo cho Restart=on-failure
    của systemd khởi động lại. Huỷ các vòng còn sống rồi raise lại ở đây khiến cả tiến trình
    thoát với mã khác 0, để systemd khởi động lại cả ba vòng gọn gàng - đúng bảo đảm mà bản
    một-consumer trước đây đã có."""
    tasks = {
        asyncio.create_task(_run_platform_consumer(platform, group_id), name=platform): platform
        for platform, group_id in PLATFORM_CONSUMER_GROUPS.items()
    }
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError, KeyboardInterrupt:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("crawl_request_consumer_stopped")
        return

    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)

    finished = next(iter(done))
    platform = tasks[finished]
    if finished.cancelled():
        logger.info("crawl_request_consumer_stopped", platform=platform)
        return
    exc = finished.exception()
    if isinstance(exc, (KeyboardInterrupt, asyncio.CancelledError)):
        logger.info("crawl_request_consumer_stopped", platform=platform)
        return
    logger.error("platform_consumer_stopped", platform=platform, error=str(exc) if exc else None)
    raise RuntimeError(f"{platform} consumer loop stopped unexpectedly") from exc


if __name__ == "__main__":
    # Trang Nhật ký + cảnh báo lỗi crawl của dashboard đọc file này (cinemark-api GET
    # /logs/spider-hub). CONSUMER_LOG_FILE để đổi chỗ, đặt rỗng để tắt.
    _log_file = os.getenv("CONSUMER_LOG_FILE", str(REPO_ROOT / "consumer.log"))
    if _log_file:
        enable_file_logging(_log_file)
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(0)
