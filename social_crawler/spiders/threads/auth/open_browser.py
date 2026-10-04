"""Mở một trình duyệt có giao diện đã đăng nhập vào session sẵn có của tài khoản Threads đang
được xoay tới, để người dùng xem / xử lý một thử thách checkpoint trong trình duyệt mà Meta
đưa ra giữa phiên (xem body response `checkpoint_required` trong graphql_client.py) - tách
khỏi luồng đăng nhập+bắt đầy đủ của bootstrap.py, vốn giả định tài khoản đã trả lời request
tìm kiếm bình thường và nếu không thì sẽ cố chạy lại việc bắt search với một tài khoản
chưa tìm kiếm được.

Chạy:
    python -m social_crawler.spiders.threads.auth.open_browser

Dùng lại storage_state nào đã cache cho tài khoản được xoay tới (cùng cái mà các lần chạy
bình thường của bootstrap.py dùng) - nếu chưa có, quay về luồng đăng nhập của chính
bootstrap.py. Để trình duyệt mở cho tới khi bạn nhấn Enter ở đây, rồi lưu cookie kết quả
trở lại Redis để lần chạy `bootstrap.py --query "..."` tiếp theo dùng session đã được xử
lý thay vì session bị checkpoint.
"""

from __future__ import annotations

from patchright.sync_api import sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.threads import STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.spiders.threads.auth.bootstrap import _get_authenticated_context

logger = get_logger(__name__)


def main() -> None:
    redis_cache = RedisCache()
    with sync_playwright() as pw:
        browser, context, page, account_key, _account = _get_authenticated_context(pw, redis_cache, headless=False)
        try:
            page.goto("https://www.threads.com/")
            logger.info(
                "browser_open_for_manual_inspection",
                account=account_key,
                hint="resolve any checkpoint/verification prompt here, then press Enter in this terminal",
            )
            input()
        finally:
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()
            logger.info("saved_session_after_manual_inspection", account=account_key)


if __name__ == "__main__":
    main()
