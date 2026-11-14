"""Facebook dùng danh tính trình duyệt gốc của runtime. Không nâng UA, không giả lập hệ điều hành.

Target curl chỉ chọn một cách cài transport được hỗ trợ; không chứng minh
hành vi TLS/HTTP2 giống hệt Chromium. Header request đã bắt được giữ nguyên.
"""

from __future__ import annotations

import base64
import re
import sys

from curl_cffi.requests.impersonate import BrowserTypeLiteral

IDENTITY_KEY = "facebook:browser_identity:{account}"


class BrowserIdentityError(RuntimeError):
    """Phiên khôi phục cần thiết lập rõ trên runtime tương thích."""


def runtime_system() -> str:
    if sys.platform == "darwin":
        return "macOS"
    if sys.platform == "win32":
        return "Windows"
    if sys.platform.startswith("linux"):
        return "Linux"
    raise BrowserIdentityError("Unsupported Facebook browser runtime")


def ua_system(ua: str) -> str:
    if "Android" in ua or "Mobile" in ua:
        raise BrowserIdentityError("Mobile Facebook sessions require their original runtime")
    if "Windows" in ua:
        return "Windows"
    if "Macintosh" in ua:
        return "macOS"
    if "Linux" in ua:
        return "Linux"
    raise BrowserIdentityError("Missing or unsupported Facebook browser identity")


def transport_profile(headers: dict) -> dict:
    """Profile curl_cffi cho phiên HTTP khôi phục từ UA đã bắt.

    Không kiểm hệ điều hành máy chạy (10/11/2026): curl_cffi giả vân tay TLS/HTTP2 của trình duyệt đã bắt
    (``chromeNNN_macos`` với UA Mac) dù crawler chạy trên hệ điều hành nào, nên phiên bắt từ Chrome trên
    Mac phát lại được từ VPS Linux với cùng UA, vân tay và proxy. Còn trình duyệt thật
    (new_facebook_context) vẫn phải chạy trên đúng hệ điều hành của phiên.
    """
    ua = headers.get("user-agent") or ""
    system = ua_system(ua)
    match = re.search(r"(?:Headless)?Chrome/(\d+)", ua)
    if not match or "Edg/" in ua or "OPR/" in ua:
        raise BrowserIdentityError("Unsupported Facebook browser version; refusing to substitute a profile")
    target = f"chrome{match[1]}" + ("_macos" if system == "macOS" else "")
    if target not in BrowserTypeLiteral.__args__:
        raise BrowserIdentityError("No supported HTTP transport for this captured browser version")
    hint = str(headers.get("sec-ch-ua-platform") or "").strip('"')
    if hint and hint != system:
        raise BrowserIdentityError("Captured Facebook UA and platform headers disagree")
    return {"target": target, "user_agent": ua}


def stored_ua(cache, account: str) -> str | None:
    from social_crawler.constants.facebook import CACHE_REDIS_KEY_TMPL

    identity = cache.get(IDENTITY_KEY.format(account=account)) or {}
    recipe = cache.get(CACHE_REDIS_KEY_TMPL.format(account=account)) or {}
    return identity.get("user_agent") or recipe.get("headers", {}).get("user-agent")


def new_facebook_context(browser, account_key=None, *, redis_cache=None, **kwargs):
    """Kiểm trên about:blank trước khi nạp cookie hay gọi Facebook.

    Đăng nhập tay mới dùng mặc định gốc. Khôi phục cần đúng UA đã ghi;
    runtime/phiên bản/chế độ khác là lỗi thiết lập, không phải tài khoản chết.
    """
    expected = kwargs.pop("user_agent", None)
    state = kwargs.get("storage_state") or {}
    if not isinstance(state, dict):
        raise BrowserIdentityError("Facebook Restore requires an inspected state object, not a path")
    restoring = bool(state.get("cookies"))
    if redis_cache is not None and account_key:
        expected = stored_ua(redis_cache, account_key) or expected
    if any(key in kwargs for key in ("screen", "device_scale_factor", "extra_http_headers")):
        raise BrowserIdentityError("Facebook native contexts do not accept device/header overrides")
    probe = browser.new_context()
    try:
        page = probe.new_page()
        native_ua = page.evaluate("navigator.userAgent")
        system = ua_system(native_ua)
        if system != runtime_system():
            raise BrowserIdentityError("Browser UA does not describe the current OS")
        if restoring and (not expected or native_ua != expected):
            raise BrowserIdentityError(
                "Restore browser identity differs or is unknown; use explicit login/setup on this runtime"
            )
    finally:
        probe.close()
    context = browser.new_context(no_viewport=True, **kwargs)
    context._facebook_native_identity = {"user_agent": native_ua, "system": system}
    context._facebook_restored = restoring
    context._facebook_identity_saved = False
    return context


def save_native_state(cache, account, context):
    from social_crawler.spiders.facebook.auth.cookies import RECIPE_KEY_TEMPLATES, save_browser_state

    state = context.storage_state()
    identity = context._facebook_native_identity
    cookies = {c["name"]: c["value"] for c in state.get("cookies", [])}
    if cookies.get("c_user") not in (None, "", "deleted"):
        new_login = not context._facebook_restored and not context._facebook_identity_saved
        # Đăng nhập mới thành công sẽ làm phiên trước mất hiệu lực, kể cả cùng hệ điều hành.
        for template in RECIPE_KEY_TEMPLATES:
            key = template.format(account=account)
            recipe = cache.get(key)
            if recipe and (new_login or recipe.get("headers", {}).get("user-agent") != identity["user_agent"]):
                cache.delete(key)
        cache.set(IDENTITY_KEY.format(account=account), identity)
        context._facebook_identity_saved = True
    save_browser_state(cache, account, state)


def native_cookie_header(header, context):
    from social_crawler.spiders.facebook.auth.cookies import parse_cookie_header

    cookies = parse_cookie_header(header)
    cookies["useragent"] = base64.b64encode(context._facebook_native_identity["user_agent"].encode()).decode()
    return "; ".join(f"{key}={value}" for key, value in cookies.items())
