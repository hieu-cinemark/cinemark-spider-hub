"""Kiểm tra môi trường đăng nhập mà không chạm tài khoản hay Facebook: mở đúng trình duyệt mà mọi đường xác thực dùng (patchright,
BROWSER_CHROMIUM_CHANNEL) trên about:blank, so phiên bản thật với hồ sơ dấu vân tay, và thử Redis. deploy-repo.sh chạy script
này TRƯỚC khi restart service; thiếu binary hay lệch phiên bản thì dừng ở đây thay vì để job đầu tiên lỗi trên tài khoản thật.

    python -m scripts.preflight_browser
"""

from __future__ import annotations

import sys

from patchright.sync_api import sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.core.fingerprint import CHROME_MAJOR
from social_crawler.spiders.facebook.auth.browser_interaction import launch_chromium


def main() -> int:
    with sync_playwright() as pw:
        browser = launch_chromium(pw, headless=True)
        try:
            page = browser.new_page()
            page.goto("about:blank")
            version = browser.version
        finally:
            browser.close()
    print(f"[preflight] chromium {version}, fingerprint profile Chrome {CHROME_MAJOR}", flush=True)
    if version.split(".")[0] != str(CHROME_MAJOR):
        print("[preflight] FAILED: browser version does not match the fingerprint profile", file=sys.stderr)
        return 1
    if not RedisCache().ping():
        print("[preflight] FAILED: redis unreachable", file=sys.stderr)
        return 1
    print("[preflight] ok", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
