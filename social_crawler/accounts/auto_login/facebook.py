"""Một lần đăng nhập Facebook tự động bằng thông tin đăng nhập cho một tài khoản - điền
form đăng nhập và tự giải 2FA (auto_login/submit_two_factor_code trong
facebook/auth/triggers.py), rồi ghi cookie vừa lấy được thẳng vào cột
platform_accounts.cookie của tài khoản đó (và cache storage_state trong Redis của nó,
để lượt crawl/bootstrap ngay sau đó dùng lại được luôn, không cần làm gì thêm).

Dùng chung cho orchestrator auto-login (orchestrator.py) và script theo lô của người
vận hành (scripts/relogin_facebook_accounts.py), để chỉ có một phiên bản "cách đăng
nhập Facebook tự động" thay vì hai bản lệch dần nhau.
"""

from __future__ import annotations

from collections.abc import Callable

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import ACTIVE_ACCOUNT_REDIS_KEY
from social_crawler.db.accounts import Account, get_account_pk, record_cookie_check, update_account_cookie
from social_crawler.runtime import account_lease, pool
from social_crawler.spiders.facebook.auth.browser_interaction import has_display, launch_chromium
from social_crawler.spiders.facebook.auth.cookies import (
    REQUIRED_LOGIN_COOKIES,
    keep_user_agent,
    parse_cookie_header,
)
from social_crawler.spiders.facebook.auth.cookies import (
    extract_facebook_user_agent as extract_user_agent,
)
from social_crawler.spiders.facebook.auth.identity import native_cookie_header, save_native_state
from social_crawler.spiders.facebook.auth.identity import new_facebook_context as new_context
from social_crawler.spiders.facebook.auth.triggers import (
    MissingTotpSecretError,
    TwoFactorPromptNotHandledError,
    auto_login,
)

PLATFORM = "facebook"


def relogin_one(
    pw,
    redis_cache: RedisCache,
    account: Account,
    *,
    headless: bool | None = None,
    code_provider: Callable[[], str | None] | None = None,
) -> tuple[str, str | None]:
    """_relogin_leased trong lease của tài khoản: job khác đang dùng thì không đăng nhập (status "error", lượt sau xét lại)."""
    account_key = (account.get("email") or account["id"]).strip().lower()
    try:
        with account_lease.hold(PLATFORM, account_key, "auto_login", redis_cache):
            return _relogin_leased(pw, redis_cache, account, headless=headless, code_provider=code_provider)
    except account_lease.AccountBusyError as exc:
        return "error", str(exc)


def _relogin_leased(
    pw,
    redis_cache: RedisCache,
    account: Account,
    *,
    headless: bool | None = None,
    code_provider: Callable[[], str | None] | None = None,
) -> tuple[str, str | None]:
    """Trả về (status, note). status là "relogged_in"/"needs_human"/"failed"/"error".

    account có dạng db.accounts.Account (account["id"] là định danh đăng nhập, "2fa" là
    secret TOTP). headless=None chỉ mở trình duyệt có giao diện khi máy có màn hình.
    code_provider cung cấp mã 2FA cho tài khoản không có secret TOTP (xem
    triggers.submit_two_factor_code). Khi thành công, lần kiểm tra gần nhất của tài khoản
    được ghi là "alive", nên nó rơi khỏi mọi danh sách đăng nhập lại theo trạng thái
    "dead"."""
    account_key = (account.get("email") or account["id"]).strip().lower()

    old_cookies = parse_cookie_header(account.get("cookie") or "")
    account_user_agent = extract_user_agent(old_cookies)
    context_kwargs = {"user_agent": account_user_agent} if account_user_agent else {}

    if not account.get("password"):
        return "needs_human", "no password stored on this row - cannot auto-login"
    try:
        # Cùng chính sách proxy như auto-login của bootstrap.py: chỉ dùng proxy đã ghim của
        # chính tài khoản, không bao giờ không proxy hay một proxy chưa ghim - session mà lần
        # đăng nhập này tạo ra được dùng lại qua đúng proxy đã ghim đó.
        proxy = pool.pinned_login_proxy(PLATFORM, account_key)
        pool.check_login_egress(PLATFORM, account_key, proxy, redis_cache)
    except RuntimeError as exc:
        return "error", f"no usable pinned proxy: {exc}"

    browser = launch_chromium(pw, headless=not has_display() if headless is None else headless, proxy=proxy)
    try:
        context = new_context(browser, account_key=account_key, redis_cache=redis_cache, **context_kwargs)
        page = context.new_page()
        try:
            auto_login(page, account, code_provider=code_provider)
        except MissingTotpSecretError:
            return "needs_human", "2FA prompt shown, no totp_secret on this row and no code from its email"
        except TwoFactorPromptNotHandledError as exc:
            return "needs_human", f"2FA prompt shown but not recognized: {exc}"
        except Exception as exc:
            return "error", f"login flow crashed: {exc}"

        cookies_after = context.cookies()
        cookie_names = {c["name"] for c in cookies_after}
        if not all(name in cookie_names for name in REQUIRED_LOGIN_COOKIES):
            debug_path = f"debug_relogin_failed_{account['id']}.png"
            page.screenshot(path=debug_path)
            return "needs_human", f"no c_user after login - possible checkpoint, see {debug_path}"

        cookie_header = keep_user_agent(
            "; ".join(f"{c['name']}={c['value']}" for c in cookies_after if "facebook.com" in c["domain"]),
            account.get("cookie") or "",
        )
        cookie_header = native_cookie_header(cookie_header, context)
        row_id = get_account_pk(PLATFORM, account["id"])
        if row_id is None:
            return "error", "could not resolve this account's row id to write the new cookie back"
        if not update_account_cookie(PLATFORM, row_id, cookie_header):
            return "error", "login succeeded but writing the new cookie to Supabase failed"

        save_native_state(redis_cache, account_key, context)
        redis_cache.set(ACTIVE_ACCOUNT_REDIS_KEY, account_key)
        # Để dashboard và lần chạy check_facebook_cookies.py tiếp theo phản ánh ngay - và để bộ
        # lập lịch/consumer auto-login thôi liệt kê nó là chết rồi đăng nhập lại ở mỗi lượt.
        record_cookie_check(PLATFORM, account["id"], status="alive", note=None)
        return "relogged_in", None
    finally:
        browser.close()
