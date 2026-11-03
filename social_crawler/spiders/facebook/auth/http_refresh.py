"""HTTP-only refresh and public Relay discovery. Never executes downloaded JavaScript."""

from __future__ import annotations

import html
import json
import os
import re
import time
from urllib.parse import urljoin, urlsplit

from curl_cffi import requests

from social_crawler.accounts.account_warning import find_marker
from social_crawler.core.logger import get_logger
from social_crawler.spiders.facebook.auth.cookies import RECIPE_KEY_TEMPLATES, RECIPE_TTL, save_http_cookies

logger = get_logger(__name__)

PUBLIC_DOCS_KEY = "facebook:public_operations:v1"
PUBLIC_PROXY_ID = int(os.getenv("FACEBOOK_PUBLIC_DISCOVERY_PROXY_ID", "15"))
RESTRICTION_ERROR_CODES = {"368", "1675004"}
RESTRICTION_PHRASES = ("rate limit exceeded", "misusing this feature")


def session_logged_out(headers) -> bool:
    values = {str(k).lower(): str(v).lower() for k, v in headers.items()}
    return bool(re.search(r'(?:^|[,;]\s*)c_user\s*=\s*"?deleted\b', values.get("set-cookie", ""))) or (
        "logged-out" in values.get("set-login", "")
    )


def redirects_to_checkpoint(headers) -> bool:
    values = {str(k).lower(): str(v) for k, v in headers.items()}
    return "/checkpoint/" in values.get("location", "")


def extract_tokens(text: str) -> dict[str, str]:
    result = {}
    for key, patterns in {
        "fb_dtsg": [
            r'"DTSGInitialData",\[\],\{"token":"([^"]+)"',
            r'"name":"fb_dtsg","value":"([^"]+)"',
            r'"dtsgToken"\s*:\s*"([^"]+)"',
        ],
        "lsd": [r'"LSD",\[\],\{"token":"([^"]+)"', r'"name":"lsd","value":"([^"]+)"'],
        "__rev": [r'"__rev"\s*:\s*(\d+)', r'"server_revision"\s*:\s*(\d+)'],
        "__hsi": [r'"hsi"\s*:\s*"(\d+)"'],
    }.items():
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                result[key] = json.loads('"' + match[1] + '"')
                break
    return result


def classify_error(parsed) -> str | None:
    if not isinstance(parsed, dict):
        return None
    messages = [str(e.get("message", "")).lower() for e in (parsed.get("errors") or []) if isinstance(e, dict)]
    if any("missing_required_variable_value" in m or "variable" in m and "invalid value" in m for m in messages):
        return "schema"
    if any("graphql document with id" in m and "was not found" in m for m in messages):
        return "doc_id"
    if parsed.get("errors") or parsed.get("error"):
        return "unknown"
    return None


def _error_fields(parsed) -> tuple[list[str], list[str]]:
    """(mã lỗi, câu thông báo) chỉ từ trường lỗi có cấu trúc - không đọc `data` hay HTML, nơi có sẵn nội dung bài viết và
    chuỗi giao diện trong bundle."""
    if not isinstance(parsed, dict):
        return [], []
    errors = [e for e in (parsed.get("errors") or []) if isinstance(e, dict)]
    codes = [str(parsed.get("error"))] if parsed.get("error") else []
    texts = [str(parsed[k]) for k in ("errorSummary", "errorDescription") if parsed.get(k)]
    for error in errors:
        codes += [str(error[k]) for k in ("code", "api_error_code") if error.get(k)]
        texts += [str(error[k]) for k in ("message", "summary", "description") if error.get(k)]
    return codes, texts


def restriction_signal(parsed) -> str | None:
    """Tín hiệu Facebook đang hạn chế tài khoản trong phản hồi GraphQL. Mã và câu chữ lấy theo tài liệu/ghi nhận công khai,
    chưa bắt được mẫu thật trên hệ thống này (03/11/2026) - bổ sung từ log graphql_error_unclassified."""
    codes, texts = _error_fields(parsed)
    code = next((c for c in codes if c in RESTRICTION_ERROR_CODES), None)
    if code:
        return f"error_{code}"
    text = " ".join(texts)
    lowered = text.lower()
    return next((p for p in RESTRICTION_PHRASES if p in lowered), None) or find_marker(text)


def error_summary(parsed) -> dict[str, list[str]]:
    codes, texts = _error_fields(parsed)
    return {"codes": codes[:5], "messages": [t[:120] for t in texts[:5]]}


def operations_in_script(text: str) -> dict[str, str]:
    # Scope IDs to their own Relay operation module; never guess a nearby module's ID.
    result = {}
    for match in re.finditer(r'__d\(\s*"(\w+)_facebookRelayOperation"\s*,', text):
        end = text.find("__d(", match.end())
        module = text[match.end() : end if end != -1 else len(text)]
        identifier = re.search(r'(?:exports\s*=|\b[a-zA-Z_$][\w$]*\s*=)\s*"(\d{10,})"', module[:2000])
        if identifier:
            result[match[1]] = identifier[1]
    for name, identifier in re.findall(r'"(\w+)_facebookRelayOperation"\s*:\s*"(\d{10,})"', text):
        result[name] = identifier
    return result


def public_proxy(proxy_id: int = PUBLIC_PROXY_ID):
    from social_crawler.db.connection import connect
    from social_crawler.runtime.pool import build_proxy_url

    with connect() as conn:
        row = conn.execute("SELECT * FROM platform_proxies WHERE id = %s AND enabled = true", (proxy_id,)).fetchone()
        if not row:
            raise RuntimeError("Public discovery proxy is missing or disabled")
        # Even disabled accounts count: never borrow an account's assigned endpoint.
        assigned = conn.execute(
            "SELECT 1 FROM platform_accounts pa JOIN platform_proxies pp ON pp.id = pa.assigned_proxy_id "
            "WHERE pp.proxy_url = %s LIMIT 1",
            (row["proxy_url"],),
        ).fetchone()
        if assigned:
            raise RuntimeError("Public discovery proxy is assigned to an account")
        cfg = {"url": row["proxy_url"], "username": row.get("username"), "password": row.get("password")}
    return build_proxy_url(cfg)


def discover_public(cache, *, proxy_id=PUBLIC_PROXY_ID, pages=None) -> dict[str, str]:
    # A failed discovery also cools down: many jobs must not repeatedly fetch public bundles.
    if not cache.set_nx("facebook:public_discovery_cooldown", 1, ttl_seconds=1800):
        return (cache.get(PUBLIC_DOCS_KEY) or {}).get("operations", {})
    proxy = public_proxy(proxy_id)
    deadline = time.monotonic() + 75
    found = {}
    # Independent, empty cookie jar: no account cache, cookie, auth header or browser state.
    with requests.Session(impersonate="chrome", proxies={"http": proxy, "https": proxy}, trust_env=False) as client:

        def fetch(url):
            for _ in range(4):
                host = urlsplit(url)
                if (
                    host.scheme != "https"
                    or host.port not in (None, 443)
                    or host.username
                    or host.password
                    or not (
                        host.hostname in ("www.facebook.com", "facebook.com", "m.facebook.com")
                        or (host.hostname or "").endswith(".fbcdn.net")
                    )
                ):
                    raise RuntimeError("Public discovery URL is outside allowed hosts")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Public discovery deadline reached")
                client.cookies.clear()
                response = client.get(url, timeout=min(15, remaining), allow_redirects=False)
                if response.status_code in (301, 302, 303, 307, 308):
                    url = urljoin(url, response.headers.get("location", ""))
                    continue
                response.raise_for_status()
                if len(response.content) > 5_000_000:
                    raise RuntimeError("Public discovery response is too large")
                return response.text
            raise RuntimeError("Too many public discovery redirects")

        scripts = []
        for page in pages or [
            "https://www.facebook.com/",
            "https://www.facebook.com/search/posts/?q=cinema",
            "https://www.facebook.com/Cristiano/",
        ]:
            try:
                text = fetch(page)
            except (requests.RequestsError, RuntimeError, TimeoutError) as exc:
                logger.warning(
                    "public_doc_page_unavailable", status=getattr(getattr(exc, "response", None), "status_code", None)
                )
                continue
            found.update(operations_in_script(text))
            scripts.extend(re.findall(r'<script\b[^>]*\bsrc=["\']([^"\']+)', text))
            scripts.extend(re.findall(r'"src"\s*:\s*"(https:[^"]+\.js[^" ]*)"', text))
        for src in list(dict.fromkeys(scripts))[:30]:
            src = html.unescape(src.replace("\\/", "/"))
            try:
                found.update(operations_in_script(fetch(urljoin("https://www.facebook.com/", src))))
            except requests.RequestsError, RuntimeError, TimeoutError:
                continue
    if not found:
        logger.warning("public_doc_discovery_empty", proxy_id=proxy_id)
        return {}
    cache.set(PUBLIC_DOCS_KEY, {"captured_at": time.time(), "operations": found}, ttl_seconds=86400)
    return found


def save_tokens(client, tokens: dict[str, str]) -> None:
    cookies = dict(client._cache["cookies"])
    jar = client._session.cookies.get_dict()
    if isinstance(jar, dict):
        if jar.get("c_user", cookies.get("c_user")) != cookies.get("c_user"):
            raise RuntimeError("HTTP response changed account identity; refusing to save")
        cookies.update(jar)
    save_http_cookies(client._redis, client._account, cookies)
    cookies = {name: value for name, value in cookies.items() if value != "deleted"}
    for template in RECIPE_KEY_TEMPLATES:
        key = template.format(account=client._account)
        recipe = client._redis.get(key)
        if not recipe:
            continue
        recipe = {
            **recipe,
            "body_static": {**recipe.get("body_static", {}), **tokens},
            "cookies": cookies,
            "http_refreshed_at": time.time(),
        }
        client._redis.set(key, recipe, ttl_seconds=RECIPE_TTL)
    # Giữ dấu vân tay đã chuẩn hoá của client: công thức trong Redis mang UA/Client Hints lúc bắt.
    client._cache = {
        **client._cache,
        "body_static": {**client._cache["body_static"], **tokens},
        "cookies": cookies,
        "http_refreshed_at": time.time(),
    }


def refresh(client) -> bool:
    client._throttle()
    response = client._session.get(
        "https://www.facebook.com/",
        cookies=client._cache["cookies"],
        headers=client._html_headers(),
        timeout=client.REQUEST_TIMEOUT,
        allow_redirects=False,
    )
    client._check_session_headers(response)
    if response.status_code != 200:
        return False
    tokens = extract_tokens(response.text)
    # A token alone is not evidence of an authenticated user; refuse login/checkpoint pages.
    identity = re.search(r'"USER_ID"\s*:\s*"(\d+)"', response.text)
    if not identity or identity[1] != str(client._cache["cookies"].get("c_user")) or not tokens.get("fb_dtsg"):
        return False
    save_tokens(client, tokens)
    return True


def main():
    import argparse

    from social_crawler.clients.redis import RedisCache

    parser = argparse.ArgumentParser(description="Discover public Relay IDs without account cookies")
    parser.add_argument("--proxy-id", type=int, default=PUBLIC_PROXY_ID)
    args = parser.parse_args()
    operations = discover_public(RedisCache(), proxy_id=args.proxy_id)
    targets = {
        name: identifier
        for name, identifier in operations.items()
        if any(part in name.lower() for part in ("search", "comments", "depth1", "depth2"))
    }
    print(json.dumps({"operations_found": len(operations), "target_operations": targets}, ensure_ascii=False))


if __name__ == "__main__":
    main()
