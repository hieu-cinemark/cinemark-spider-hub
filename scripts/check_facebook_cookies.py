"""Kiểm tra thụ động xem cookie đã cache của các platform_accounts facebook còn sống
không - không dùng mật khẩu/2FA/đăng nhập gì cả. Dùng lại trường `cookie` sẵn có của
mỗi tài khoản (y như đường "dùng lại session đã cache" của một lượt crawl thật trong
facebook/auth/bootstrap.py) để tải một trang Facebook thật qua proxy đã ghim của tài
khoản đó, rồi kiểm tra session còn được chấp nhận (vẫn còn cookie c_user, không bị
chuyển hướng tới trang đăng nhập/checkpoint) hay đã chết (session bị vô hiệu - Facebook
đã đăng xuất nó phía server).

Cố ý không bao giờ thử đăng nhập mới - đăng nhập lại là việc của auto_login (luôn qua
proxy đã ghim của tài khoản, xem auto_login/orchestrator.py). Một sự cố thật đã ghi
nhận: một tài khoản bị gắn cờ "suspected automated behavior" vì đăng nhập tự động. Chạy
script này trước, trên một lô tài khoản vừa thêm, để biết tài khoản nào thực sự cần
đăng nhập lại trước khi tốn công cho việc đó - phần lớn tài khoản có cookie đã chứa
c_user/xs thì không cần đăng nhập gì cả.

Chạy lần lượt từng tài khoản, nghỉ ngẫu nhiên giữa các lần (không song song/liên tục)
- với số proxy ít như hiện tại, nhiều tài khoản sẽ rơi vào cùng một proxy đã ghim, và
một loạt lượt tải trang đổi danh tính liên tục từ một IP tự nó đã là dấu hiệu đáng ngờ,
bất kể có dùng mật khẩu hay không.

Cách dùng:
    python -m scripts.check_facebook_cookies
    python -m scripts.check_facebook_cookies --account 61570510702486
"""

from __future__ import annotations

import argparse
import random
import sys
import time

from patchright.sync_api import sync_playwright

from social_crawler.db.accounts import get_accounts, record_cookie_check
from social_crawler.logger import get_logger
from social_crawler.services import pool
from social_crawler.spiders.facebook.auth.browser_interaction import new_context
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    build_storage_state_from_cookies,
    parse_cookie_header,
)

logger = get_logger(__name__)

PLATFORM = "facebook"
# Giữa các tài khoản - cố ý không chạy liên tục, xem docstring module.
_MIN_PAUSE_SECONDS = 4.0
_MAX_PAUSE_SECONDS = 10.0


def _check_one(pw, account: dict) -> tuple[str, str | None]:
    """Trả về (status, note). status là một trong "alive"/"dead"/"skipped"/"error" - "error"
    nghĩa là bản thân việc kiểm tra không chạy được (không có proxy, trình duyệt crash,
    ...), không phải đã xác nhận cookie chết."""
    account_key = (account.get("email") or account["id"]).strip().lower()
    cookie = account.get("cookie") or ""
    if not cookie:
        return "skipped", "no cookie on this row"

    cookie_names = set(parse_cookie_header(cookie).keys())
    missing = [name for name in REQUIRED_LOGIN_COOKIES if name not in cookie_names]
    if missing:
        return "dead", f"missing required cookie(s) {missing}"

    proxy = None
    try:
        # required=True - cùng lý do như đường crawl thật (xem facebook/auth/bootstrap.py): việc
        # này dùng lại session có sẵn, là lưu lượng thường ngày, không phải trình duyệt đăng nhập
        # một lần có chủ đích.
        proxy_cfg = pool.acquire_proxy_for_account(PLATFORM, account_key, required=True)
    except pool.ProxyPoolExhaustedError as exc:
        return "error", f"no usable proxy: {exc}"
    if proxy_cfg and proxy_cfg["login_use_proxy"]:
        proxy = {
            "server": f"http://{proxy_cfg['url']}",
            "username": proxy_cfg["username"],
            "password": proxy_cfg["password"],
        }

    storage_state = build_storage_state_from_cookies(cookie)
    browser = pw.chromium.launch(headless=True, proxy=proxy)
    try:
        context = new_context(browser, account_key=account_key, storage_state=storage_state)
        page = context.new_page()
        try:
            page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
        except Exception as exc:
            return "error", f"page load failed: {exc}"
        page.wait_for_timeout(2000)

        still_has_c_user = any(c["name"] == "c_user" for c in context.cookies())
        bounced = "login" in page.url or "checkpoint" in page.url

        if still_has_c_user and not bounced:
            return "alive", None
        return "dead", f"c_user_present={still_has_c_user} url={page.url}"
    finally:
        browser.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--account", help="Only check this one account_id/email instead of every enabled facebook account"
    )
    args = parser.parse_args()

    # Xem bản sửa giống hệt trong relogin_facebook_accounts.py để biết vì sao điều này quan
    # trọng với mọi lượt chạy nền/qua pipe: không có nó, một lượt đang chạy bình thường và
    # một lượt thật sự kẹt ở tài khoản đầu tiên trông giống hệt nhau trong file log suốt
    # nhiều phút.
    sys.stdout.reconfigure(line_buffering=True)

    accounts = get_accounts(PLATFORM)
    if args.account:
        needle = args.account.strip().lower()
        accounts = [a for a in accounts if a["id"] == args.account or (a.get("email") or "").lower() == needle]

    if not accounts:
        print("No enabled facebook accounts to check.")
        return

    print(f"Checking {len(accounts)} facebook account(s) - one at a time, paced apart...\n")
    results: list[tuple[str, str, str | None]] = []
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            label = account.get("email") or account["id"]
            try:
                status, note = _check_one(pw, account)
            except Exception as exc:
                status, note = "error", str(exc)
                logger.error("cookie_check_crashed", account=label, error=str(exc))
            if status in ("alive", "dead"):
                record_cookie_check(PLATFORM, account["id"], status=status, note=note)
            results.append((label, status, note))
            print(f"  {label:45s} {status:8s} {note or ''}")
            if i < len(accounts) - 1:
                time.sleep(random.uniform(_MIN_PAUSE_SECONDS, _MAX_PAUSE_SECONDS))

    alive = sum(1 for _, s, _ in results if s == "alive")
    dead = sum(1 for _, s, _ in results if s == "dead")
    other = len(results) - alive - dead
    print(f"\n{alive} alive, {dead} dead, {other} skipped/error (out of {len(results)}).")
    if dead:
        print("Dead accounts need a real human-supervised re-login:")
        print(
            "  python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual --account <email_or_id>"
        )


if __name__ == "__main__":
    main()
