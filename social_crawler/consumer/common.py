"""Hằng số, lỗi job và đường dẫn dùng chung của consumer."""

from __future__ import annotations

import sys
from pathlib import Path

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE
from social_crawler.logger import get_logger
from social_crawler.services import pool

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


SCRAPY_BIN = str(Path(sys.executable).parent / "scrapy")


PYTHON_BIN = sys.executable


REPO_ROOT = Path(__file__).resolve().parents[2]


SPIDER_BY_PLATFORM = {"facebook": "facebook_search", "threads": "threads_search"}


TIKTOK_HASHTAG_SPIDER = "tiktok_hashtag_search"


TIKTOK_KEYWORD_SPIDER = "tiktok_keyword_search"


TIKTOK_CHANNEL_VIDEOS_SPIDER = "tiktok_channel_videos"


FACEBOOK_PAGE_FEED_SPIDER = "facebook_page_feed"


# Worker hồ sơ kênh Threads (follower) - xem _run_threads_profiles.
THREADS_PROFILES_MODULE = "social_crawler.spiders.threads.features.profiles.profiles"


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
