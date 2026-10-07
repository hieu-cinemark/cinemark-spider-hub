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
    python -m scripts.check_facebook_cookies --stale-hours 6   # cron: chỉ tài khoản chưa kiểm tra trong 6 giờ

Chạy theo lịch (2026-10-07): bộ lập lịch của cinemark-api xếp một request type=cookie_check vào crawl_requests
mỗi cookie_check_interval_hours (cài đặt auto-login trên dashboard); crawl_request_consumer.py chạy script này
với --stale-hours. Tài khoản được ghi last_check_status='dead' sẽ được lượt auto-login kế tiếp đăng nhập lại và
nạp cookie mới. Có tài khoản chết thì gửi một cảnh báo Telegram gộp.
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from datetime import UTC, datetime, timedelta

from patchright.sync_api import sync_playwright

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import STATE_REDIS_KEY_TMPL
from social_crawler.db.accounts import (
    get_account_pk,
    get_accounts,
    last_checked_at_map,
    record_cookie_check,
    update_account_cookie,
)
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


def _open_facebook(pw, proxy: dict | None, account_key: str, storage_state: dict) -> tuple[bool, str, list[dict], dict]:
    """Mở facebook.com bằng một storage_state; trả về (còn đăng nhập?, url cuối, cookie sau khi tải, storage_state
    mới). Raise khi trang không tải được - chỗ gọi coi đó là "error", không phải cookie chết."""
    browser = pw.chromium.launch(headless=True, proxy=proxy)
    try:
        context = new_context(browser, account_key=account_key, storage_state=storage_state)
        page = context.new_page()
        page.goto("https://www.facebook.com/", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2000)
        cookies = context.cookies()
        alive = any(c["name"] == "c_user" for c in cookies) and "login" not in page.url and "checkpoint" not in page.url
        return alive, page.url, cookies, context.storage_state()
    finally:
        browser.close()


def _check_one(pw, account: dict, redis_cache: RedisCache) -> tuple[str, str | None]:
    """Trả về (status, note). status là một trong "alive"/"dead"/"skipped"/"error" - "error"
    nghĩa là bản thân việc kiểm tra không chạy được (không có proxy, trình duyệt crash,
    ...), không phải đã xác nhận cookie chết.

    Cùng thứ tự ưu tiên với crawl/nurture: storage_state trong Redis trước (phiên crawl đang dùng - sau một lần
    đăng nhập lại nó MỚI hơn cột cookie), rồi tới cột cookie của dòng nếu khác (vd. vừa dán cookie mới trên
    dashboard). Trước 2026-10-07 script chỉ đọc cột cookie, nên một tài khoản vừa đăng nhập lại (phiên mới chỉ có
    trong Redis) bị báo chết oan. Phiên còn sống thì cookie mới nhất được ghi lại vào CẢ hai chỗ."""
    account_key = (account.get("email") or account["id"]).strip().lower()
    state_key = STATE_REDIS_KEY_TMPL.format(account=account_key)
    candidates: list[tuple[str, dict]] = []
    redis_state = redis_cache.get(state_key)
    if isinstance(redis_state, dict):
        names = {c.get("name") for c in redis_state.get("cookies", [])}
        if all(name in names for name in REQUIRED_LOGIN_COOKIES):
            candidates.append(("redis", redis_state))
    cookie = account.get("cookie") or ""
    if cookie:
        db_cookies = parse_cookie_header(cookie)
        missing = [name for name in REQUIRED_LOGIN_COOKIES if name not in db_cookies]
        redis_xs = (
            next((c["value"] for c in candidates[0][1]["cookies"] if c["name"] == "xs"), None) if candidates else None
        )
        if not missing and db_cookies.get("xs") != redis_xs:
            candidates.append(("db", build_storage_state_from_cookies(cookie)))
        elif missing and not candidates:
            return "dead", f"missing required cookie(s) {missing}"
    if not candidates:
        return "skipped", "no cookie on this row"

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

    notes = []
    for source, storage_state in candidates:
        try:
            alive, url, cookies, fresh_state = _open_facebook(pw, proxy, account_key, storage_state)
        except Exception as exc:  # noqa: BLE001 - lỗi tải trang/trình duyệt = không kết luận được, không phải cookie chết
            return "error", f"page load failed ({source}): {exc}"
        if alive:
            redis_cache.set(state_key, fresh_state)
            header = "; ".join(f"{c['name']}={c['value']}" for c in cookies if "facebook.com" in c["domain"])
            row_id = get_account_pk(PLATFORM, account["id"])
            if row_id is not None and header:
                update_account_cookie(PLATFORM, row_id, header)
            return "alive", f"session from {source}"
        notes.append(f"{source}: c_user gone, url={url}")
    return "dead", "; ".join(notes)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--account", help="Only check this one account_id/email instead of every enabled facebook account"
    )
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=None,
        help="Skip accounts whose cookie was checked less than this many hours ago (used by the scheduled run)",
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

    if args.stale_hours:
        cutoff = datetime.now(tz=UTC) - timedelta(hours=args.stale_hours)
        checked = last_checked_at_map(PLATFORM)
        fresh = [a for a in accounts if checked.get(a["id"]) and checked[a["id"]] > cutoff]
        accounts = [a for a in accounts if a not in fresh]
        if fresh:
            print(f"Skipping {len(fresh)} account(s) checked within the last {args.stale_hours:g}h.")

    if not accounts:
        print("No enabled facebook accounts to check.")
        return

    print(f"Checking {len(accounts)} facebook account(s) - one at a time, paced apart...\n")
    results: list[tuple[str, str, str | None]] = []
    redis_cache = RedisCache()
    with sync_playwright() as pw:
        for i, account in enumerate(accounts):
            label = account.get("email") or account["id"]
            try:
                status, note = _check_one(pw, account, redis_cache)
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
        # Một cảnh báo gộp cho cả lượt (không phải mỗi tài khoản một tin) - auto-login sẽ thử đăng nhập lại.
        logger.warning(
            "cookie_check_found_dead",
            telegram=True,
            platform=PLATFORM,
            dead=dead,
            checked=len(results),
            accounts=", ".join(label for label, s, _ in results if s == "dead"),
        )
        print("Dead accounts are queued for auto-login (if enabled); manual fallback:")
        print(
            "  python -m social_crawler.spiders.facebook.auth.bootstrap --show-browser --manual --account <email_or_id>"
        )


if __name__ == "__main__":
    main()
