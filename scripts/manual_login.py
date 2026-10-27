"""Đăng nhập tay một tài khoản Facebook / Threads trong trình duyệt CÓ cửa sổ, qua đúng proxy đã ghim của tài khoản
(27/10/2026) - nút "Đăng nhập tay" trên spider-hub-dashboard chạy script này: trên server (màn hình ảo Xvfb :99, người
vận hành xem/thao tác qua noVNC - deploy/login-display/) hoặc trên máy có màn hình (Mac, Redis của server qua SSH tunnel).

    python -m scripts.manual_login --platform facebook --account someone@hotmail.com

Khác `bootstrap --show-browser --manual`: không chờ bấm Enter trong terminal - tự nhận ra đã đăng nhập xong (có cookie
đăng nhập, không còn ở trang login / checkpoint) rồi lưu và đóng trình duyệt. Một tab riêng (data:, khác origin nên trang
Facebook không đọc được) hiện tài khoản, mật khẩu và mã 2FA hiện tại để chép.

Redis PHẢI là Redis của server crawl (qua SSH tunnel, REDIS_HOST/REDIS_PORT): proxy được nhận qua
pool.acquire_proxy_for_account như crawler - tôn trọng "mỗi IP một tài khoản" (cổng đang bận với tài khoản khác thì
không mở), xoay IP nếu cổng vừa đổi tài khoản - và phiên được ghi thẳng vào storage_state mà crawler đọc. Kiểm tra
bằng một key chỉ server crawl có (facebook:lane:* / proxy_ip:*); Redis cục bộ trống thì dừng.

Xong: lưu storage_state, xoá cache token cũ của tài khoản (lượt crawl sau bắt token mới bằng phiên này), ghi cookie vào
platform_accounts (giữ useragent=), last_check_status='alive', ghi kết quả thành công (xoá cooldown / chuỗi lỗi), nhả
cổng proxy. Tiến trình ghi trạng thái vào logs/manual_login/<tài khoản>.json cho dashboard đọc.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

PLATFORMS = {
    "facebook": {
        "login_url": "https://www.facebook.com/login",
        "required": ("c_user", "xs"),
        "cookie_domain": "facebook.com",
        "bad_url": ("/login", "/checkpoint", "two_step_verification", "/recover"),
    },
    "threads": {
        "login_url": "https://www.threads.com/login/",
        "required": ("ds_user_id", "sessionid"),
        "cookie_domain": "threads.com",
        "bad_url": ("/login", "/challenge", "/accounts/suspended", "/checkpoint"),
    },
}
STATUS_DIR = Path(__file__).resolve().parent.parent / "logs" / "manual_login"
# Trên server chỉ có MỘT màn hình ảo (Xvfb, xem deploy/login-display/): mỗi lúc một phiên đăng nhập tay.
_LOCK_KEY = "manual_login:session"
# Màn hình ảo 1920x1200 (deploy/login-display): đủ cho viewport lớn nhất của new_context (1920x1080) + thanh trình duyệt.
_SERVER_WINDOW_ARGS = ["--window-position=0,0", "--window-size=1920,1200"]
_STABLE_SECONDS = 4
_POLL_SECONDS = 2


def _safe(key: str) -> str:
    return re.sub(r"[^a-z0-9._@-]+", "_", key.lower())[:120]


def status_path(platform: str, account_key: str) -> Path:
    return STATUS_DIR / f"{platform}_{_safe(account_key)}.json"


def _write_status(path: Path, state: str, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"state": state, "message": message, "updated_at": datetime.now(UTC).isoformat()}, ensure_ascii=False
        )
    )
    print(f"[manual_login] {state}: {message}", flush=True)


def _crawl_server_redis(cache) -> bool:
    """True khi Redis đang nối là Redis của server crawl (có key làn Facebook / quyền dùng cổng proxy)."""
    client = cache._client
    for pattern in ("facebook:lane:*", "proxy_ip:*", "facebook:storage_state:*"):
        for _ in client.scan_iter(match=pattern, count=200):
            return True
    return False


def _info_html(platform: str, account: dict, code: str | None) -> str:
    rows = [
        ("Nền tảng", platform),
        ("Tài khoản (UID)", account.get("id") or ""),
        ("Email", account.get("email") or ""),
        ("Mật khẩu", account.get("password") or ""),
        ("Mã 2FA (đổi mỗi 30 giây)", code or "(tài khoản không có 2FA)"),
    ]
    body = "".join(f"<tr><th>{html.escape(k)}</th><td><code>{html.escape(str(v))}</code></td></tr>" for k, v in rows)
    return (
        "<html><head><meta charset='utf-8'><title>Thông tin đăng nhập</title><style>"
        "body{font-family:-apple-system,sans-serif;padding:24px;background:#111827;color:#e5e7eb}"
        "th{text-align:left;padding:8px 16px 8px 0;color:#9ca3af;font-weight:500}"
        "code{font-size:16px;user-select:all}td{padding:8px 0}"
        "</style></head><body><h2>Đăng nhập tay qua proxy đã ghim</h2><table>"
        f"{body}</table><p>Đăng nhập ở tab kia. Đăng nhập xong hệ thống tự lưu phiên và đóng trình duyệt; "
        "đóng trình duyệt trước đó là huỷ.</p></body></html>"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--platform", choices=sorted(PLATFORMS), required=True)
    parser.add_argument("--account", required=True, help="email hoặc account_id trong platform_accounts")
    parser.add_argument("--timeout-minutes", type=float, default=15)
    args = parser.parse_args()

    from social_crawler.clients.redis import RedisCache
    from social_crawler.db.accounts import (
        get_account_by_key,
    )

    if args.platform == "facebook":
        from social_crawler.constants import facebook as consts
        from social_crawler.spiders.facebook.auth.accounts import account_key as normalize
    else:
        from social_crawler.constants import threads as consts
        from social_crawler.spiders.threads.auth.accounts import account_key as normalize

    cfg = PLATFORMS[args.platform]
    platform = args.platform
    account = get_account_by_key(platform, args.account)
    # Cùng khoá với bootstrap của từng nền tảng: Facebook = email (không có thì id), Threads = id.
    if account is None:
        key = normalize(args.account)
    elif platform == "facebook":
        key = normalize(account.get("email") or account["id"])
    else:
        key = normalize(account["id"])
    status = status_path(platform, key)
    if account is None:
        _write_status(status, "failed", f"Không tìm thấy tài khoản {platform} {args.account!r}")
        return 2

    cache = RedisCache()
    try:
        on_server = _crawl_server_redis(cache)
    except Exception as exc:  # noqa: BLE001 - không nối được Redis = chưa mở tunnel
        _write_status(status, "failed", f"Không nối được Redis của server crawl ({exc}). Mở SSH tunnel Redis trước.")
        return 3
    if not on_server:
        _write_status(
            status,
            "failed",
            "Redis đang nối không phải Redis của server crawl (không có key làn / cổng proxy) - "
            "mở tunnel: ssh -p 7076 -N -L 16379:127.0.0.1:6379 root@<VPS>.",
        )
        return 3

    lock_ttl = int((args.timeout_minutes + 5) * 60)
    if not cache.set_nx(_LOCK_KEY, f"{platform}:{key}", lock_ttl) and cache.get(_LOCK_KEY) != f"{platform}:{key}":
        _write_status(
            status, "failed", f"Đang có phiên đăng nhập tay khác ({cache.get(_LOCK_KEY)}) - đợi phiên đó xong."
        )
        return 7
    try:
        return _run(args, platform, account, key, status, cache, cfg, consts)
    finally:
        if cache.get(_LOCK_KEY) == f"{platform}:{key}":
            cache.delete(_LOCK_KEY)


def _run(args, platform, account, key, status, cache, cfg, consts) -> int:
    import pyotp
    from playwright.sync_api import sync_playwright

    from social_crawler.db.accounts import get_account_pk, record_cookie_check, update_account_cookie
    from social_crawler.services import ip_switch, pool
    from social_crawler.spiders.facebook.auth.browser_interaction import new_context
    from social_crawler.spiders.facebook.auth.cookies import extract_user_agent, keep_user_agent, parse_cookie_header

    _write_status(status, "starting", "Đang nhận proxy đã ghim (có thể xoay IP, tối đa ~2 phút)…")
    try:
        proxy_cfg = pool.acquire_proxy_for_account(platform, key, required=True)
    except pool.ProxyPoolExhaustedError as exc:
        _write_status(
            status, "failed", f"Chưa dùng được proxy của tài khoản lúc này (cổng đang bận / đang nghỉ): {exc}"
        )
        return 4
    if not proxy_cfg:
        _write_status(status, "failed", "Tài khoản chưa có proxy dùng được - không mở trình duyệt không proxy.")
        return 4

    proxy = {
        "server": f"http://{proxy_cfg['url']}",
        "username": proxy_cfg["username"],
        "password": proxy_cfg["password"],
    }
    old_cookie = account.get("cookie") or ""
    user_agent = extract_user_agent(parse_cookie_header(old_cookie)) if old_cookie else None
    totp = pyotp.TOTP(re.sub(r"\s+", "", account["2fa"])) if account.get("2fa") else None
    deadline = time.monotonic() + args.timeout_minutes * 60
    try:
        with sync_playwright() as pw:
            # Linux có DISPLAY = màn hình ảo của server (xem dưới noVNC) - cửa sổ phủ kín màn hình ảo.
            on_server_display = sys.platform.startswith("linux") and os.environ.get("DISPLAY", "").startswith(":")
            browser = pw.chromium.launch(
                headless=False, proxy=proxy, args=_SERVER_WINDOW_ARGS if on_server_display else []
            )
            context = new_context(browser, account_key=key, **({"user_agent": user_agent} if user_agent else {}))
            info = context.new_page()
            info.set_content(_info_html(platform, account, totp.now() if totp else None))
            page = context.new_page()
            page.goto(cfg["login_url"], wait_until="domcontentloaded", timeout=60_000)
            page.bring_to_front()
            _write_status(status, "waiting_login", f"Trình duyệt đã mở qua proxy {proxy_cfg['url']} - hãy đăng nhập.")

            logged_in_since = None
            last_code = None
            while time.monotonic() < deadline:
                if not browser.is_connected() or not context.pages:
                    _write_status(status, "failed", "Đã đóng trình duyệt trước khi đăng nhập xong - không lưu gì.")
                    return 5
                ip_switch.touch(platform, proxy_cfg, key)  # giữ quyền dùng cổng suốt lúc đăng nhập
                if totp and (code := totp.now()) != last_code and not info.is_closed():
                    last_code = code
                    info.set_content(_info_html(platform, account, code))
                names = {c["name"] for c in context.cookies()}
                urls = [p.url for p in context.pages if p.url.startswith("http")]
                ok = all(n in names for n in cfg["required"]) and not any(
                    bad in url for url in urls for bad in cfg["bad_url"]
                )
                if ok:
                    logged_in_since = logged_in_since or time.monotonic()
                    if time.monotonic() - logged_in_since >= _STABLE_SECONDS:
                        break
                else:
                    logged_in_since = None
                time.sleep(_POLL_SECONDS)
            else:
                _write_status(
                    status, "failed", f"Hết {args.timeout_minutes:g} phút mà chưa đăng nhập xong - không lưu gì."
                )
                return 6

            cookies = context.cookies()
            cache.set(consts.STATE_REDIS_KEY_TMPL.format(account=key), context.storage_state())
            # Token / query đã bắt bằng phiên cũ không còn đúng: xoá để lượt crawl sau bắt lại bằng phiên mới.
            for name in dir(consts):
                if name.endswith("_REDIS_KEY_TMPL") and name not in ("STATE_REDIS_KEY_TMPL", "THROTTLE_REDIS_KEY_TMPL"):
                    cache.delete(getattr(consts, name).format(account=key))
            header = keep_user_agent(
                "; ".join(f"{c['name']}={c['value']}" for c in cookies if cfg["cookie_domain"] in c["domain"]),
                old_cookie,
            )
            row_id = get_account_pk(platform, account["id"])
            if row_id is not None and header:
                update_account_cookie(platform, row_id, header)
            record_cookie_check(platform, account["id"], status="alive", note="đăng nhập tay qua dashboard")
            pool.release_account(platform, account["id"], success=True)
            browser.close()
    finally:
        ip_switch.release(platform, proxy_cfg, key)
    _write_status(status, "done", "Đăng nhập xong - đã lưu phiên, cookie và đánh dấu tài khoản còn sống.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
