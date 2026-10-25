"""Consumer của topic crawl_requests (Kafka): mỗi nền tảng một vòng lặp độc lập, mỗi request chạy một tiến trình con.

Mã nằm trong package social_crawler.consumer:
    loop.py      vòng lặp Kafka theo nền tảng + làn ưu tiên Redis
    dispatch.py  định tuyến request theo type
    jobs/        từng loại job (search, comments, page_feed, post_metrics, channel_videos, profiles, accounts)
    sessions.py  đảm bảo phiên đăng nhập / query đã bắt
    process.py   chạy và huỷ tiến trình con

Chạy bằng:
    python -m social_crawler.crawl_request_consumer
"""

from __future__ import annotations

import asyncio
import os
import sys

from social_crawler.consumer.common import REPO_ROOT
from social_crawler.consumer.loop import run
from social_crawler.logger import enable_file_logging

__all__ = ["run"]

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
