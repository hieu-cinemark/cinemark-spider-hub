"""Một phiên Facebook, một client (04/11/2026).

Trước đây cùng một cookie có thể đi qua Chrome của người vận hành, curl_cffi, requests và Chromium tự động, tức là một phiên hiện
ra như nhiều thiết bị. Mỗi tài khoản giờ khai báo một client trong platform_accounts.session_client và mọi đường dùng phiên đều
kiểm tra ở đây:

  manual   người vận hành tự dùng bằng trình duyệt thật; hệ thống không gửi gì
  browser  Patchright (bootstrap, nuôi) và recipe bắt từ trình duyệt, gửi lại bằng curl_cffi
  http     recipe dựng qua HTTP, gửi bằng curl_cffi với target khớp UA

Chưa khai báo thì không gửi gì: đoán sai là để lộ phiên. Slot đăng nhập tay "default" không có dòng nên không áp dụng.
"""

from __future__ import annotations

from social_crawler.constants.facebook import DEFAULT_ACCOUNT_KEY
from social_crawler.db import account_events
from social_crawler.db.accounts import session_client_of

MANUAL = "manual"
BROWSER = "browser"
HTTP = "http"
AUTOMATED = (BROWSER, HTTP)


class SessionClientMismatch(RuntimeError):
    """Không phải client đã khai báo của phiên này."""


def require(account_key: str, client: str) -> None:
    if not account_key or account_key == DEFAULT_ACCOUNT_KEY:
        return
    declared = session_client_of("facebook", account_key)
    if declared != client:
        account_events.record("facebook", account_key, "session_client_refused", wanted=client, declared=declared)
        raise SessionClientMismatch(f"Account {account_key!r} declares client {declared!r}, refusing {client!r}")


def discard_recipes_of_other_client(cache, account_key: str, recipe: dict | None) -> dict | None:
    """Đổi client thì recipe của client cũ không còn dùng được: xoá hết để client mới dựng lại, thay vì mọi job báo sai client.
    Gọi khi đang giữ lease. Chưa khai báo hoặc đang nuôi tay thì giữ nguyên, require() sẽ chặn ở bước sau."""
    from social_crawler.spiders.facebook.auth.cookies import RECIPE_KEY_TEMPLATES
    from social_crawler.spiders.facebook.auth.sdk_fallback import is_http_recipe

    if recipe is None or not account_key or account_key == DEFAULT_ACCOUNT_KEY:
        return recipe
    declared = session_client_of("facebook", account_key)
    owner = HTTP if is_http_recipe(recipe) else BROWSER
    if declared not in AUTOMATED or declared == owner:
        return recipe
    for template in RECIPE_KEY_TEMPLATES:
        cache.delete(template.format(account=account_key))
    account_events.record("facebook", account_key, "recipes_discarded", previous_client=owner, client=declared)
    return None
