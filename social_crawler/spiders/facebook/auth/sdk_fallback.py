"""Dự phòng recipe HTTP dựa trên template XD-MHLOO/facebook-gql-scraper.

Cố ý không dùng transport gốc: lease tài khoản, ngân sách request, kiểm TLS và điều kiện dừng
thuộc về spider-hub. Request đi qua
curl_cffi như mọi đường Facebook khác, với target khớp UA của phiên.
"""

from __future__ import annotations

import copy
import hashlib
import os
import time
import uuid

from curl_cffi import CurlOpt, requests

from social_crawler.constants import facebook as fb
from social_crawler.core.logger import get_logger
from social_crawler.db import account_events
from social_crawler.spiders.facebook.auth.cookies import RECIPE_TTL, build_storage_state_from_cookies
from social_crawler.spiders.facebook.auth.operation_fallback import (
    MAX_BYTES,
    SourceUnavailable,
    allowed_url,
    discover,
)

logger = get_logger(__name__)
SOURCE = "facebook-gql-scraper@3933a99a4eaac2e04754165b0ad7582b1ba452e9"
DOCS_KEY = "facebook:http_operations:{account}"
FEATURE_KEYS = {
    "search": fb.CACHE_REDIS_KEY_TMPL,
    "comments": fb.COMMENTS_REDIS_KEY_TMPL,
    "replies": fb.REPLIES_REDIS_KEY_TMPL,
    "page_feed": fb.PAGE_FEED_REDIS_KEY_TMPL,
    "page_reels": fb.PAGE_REELS_REDIS_KEY_TMPL,
}


# sec-ch-ua của trình duyệt tạo phiên, đo trên Chrome for Testing 152 có giao diện (04/11/2026): curl_cffi chrome152_macos
# báo thêm brand "Google Chrome" và thứ tự khác, nên phải gửi đúng giá trị này để cùng một phiên không đổi nhận dạng. Bản khác
# (vd. 151 của Chrome thật) giữ giá trị của target.
LOGIN_BROWSER_SEC_CH_UA = {"chrome152_macos": '"Not?A_Brand";v="24", "Chromium";v="152"'}
# Trình duyệt tải bundle như một script cross-site, không phải một lần điều hướng.
ASSET_HEADERS = {
    "accept": "*/*",
    "referer": "https://www.facebook.com/",
    "sec-fetch-dest": "script",
    "sec-fetch-mode": "no-cors",
    "sec-fetch-site": "cross-site",
    "sec-fetch-user": None,
    "upgrade-insecure-requests": None,
    "cache-control": None,
}


def enabled() -> bool:
    return os.getenv("FACEBOOK_GQL_FALLBACK_ENABLED", "1").lower() not in {"0", "false", "off", ""}


def templates():
    from facebook_gql_scraper import facebookGraphql as gql

    return {
        "search": gql.SearchCometResultsPaginatedResultsQueryGraphQL,
        "comments": gql.CommentsListComponentsPaginationQueryGraphQL,
        "replies": gql.Depth1CommentsListPaginationQueryGraphQL,
        "depth2": gql.Depth2CommentsListPaginationQueryGraphQL,
        "page_feed": gql.ProfileCometTimelineFeedRefetchQueryGraphQL,
        "page_reels": gql.ProfileCometAppCollectionReelsRendererPaginationQueryGraphQL,
    }


def session_headers(transport: dict) -> dict[str, str]:
    hints = LOGIN_BROWSER_SEC_CH_UA.get(transport["target"])
    return {"user-agent": transport["user_agent"], **({"sec-ch-ua": hints} if hints else {})}


def _transport_problems(user_agent: str) -> list[str]:
    """UA hợp lệ nhưng curl_cffi không giả được đúng bản Chrome hoặc hệ điều hành đó thì TLS sẽ lệch UA, nên không gửi."""
    from social_crawler.spiders.facebook.auth.identity import BrowserIdentityError, transport_profile

    try:
        transport_profile({"user-agent": user_agent})
    except BrowserIdentityError:
        return ["no_matching_tls_transport"]
    return []


def seed(cache, account: str) -> dict | None:
    """Đọc phiên có sẵn; không bao giờ đăng nhập, mượn cookie hay bịa UA gốc."""
    if not enabled():
        return None
    from social_crawler.db.accounts import get_account_by_key, is_account_usable, session_setup_blocked
    from social_crawler.spiders.facebook.auth.cookies import extract_facebook_user_agent, parse_cookie_header
    from social_crawler.spiders.facebook.auth.identity import IDENTITY_KEY

    if not is_account_usable("facebook", account):
        return None
    state = cache.get(fb.STATE_REDIS_KEY_TMPL.format(account=account))
    # Có trạng thái đã nhập thì nó là chuẩn, kể cả phần metadata bị thiếu.
    # Không bao giờ gắn User-Agent cũ trong DB vào phiên mới nhập.
    identity = cache.get(IDENTITY_KEY.format(account=account)) or {}
    ua = identity.get("user_agent") if state else None
    row = None
    if not state:
        if account == fb.DEFAULT_ACCOUNT_KEY:
            return None
        row = get_account_by_key("facebook", account)
        raw = parse_cookie_header((row or {}).get("cookie") or "")
        ua = extract_facebook_user_agent(raw)
        state = build_storage_state_from_cookies(raw)
    cookies = {
        c["name"]: c["value"]
        for c in state.get("cookies", [])
        if str(c.get("domain", "")).lstrip(".") in {"facebook.com", "www.facebook.com", "web.facebook.com"}
        and c.get("value") not in (None, "", "deleted")
        and (float(c.get("expires", -1)) < 0 or float(c.get("expires", -1)) > time.time())
    }
    from social_crawler.accounts.session_quality import assess

    quality = assess(cookies, ua, account)
    problems = quality.problems or _transport_problems(ua)
    if problems:
        logger.warning("facebook_cookie_refused", account=account, problems=problems)
        account_events.record("facebook", account, "cookie_refused", problems=problems, **quality.facts)
        return None
    if state and row is None and account != fb.DEFAULT_ACCOUNT_KEY:
        row = get_account_by_key("facebook", account)
        if row is None:
            return None
    if row and str(row.get("id")) != cookies["c_user"]:
        return None
    # Storage state tồn tại lâu hơn lần bị từ chối; phát lại nó sẽ làm mất tác dụng của _drop_rejected_session.
    if account != fb.DEFAULT_ACCOUNT_KEY and session_setup_blocked("facebook", account):
        logger.warning("facebook_http_setup_blocked", account=account)
        return None
    return {
        "recipe_source": SOURCE,
        "transport": "curl_cffi",
        "captured_at": time.time(),
        "request_context_id": str(uuid.uuid4()),
        "headers": {"user-agent": ua},
        "cookies": cookies,
        "body_static": {"__a": "1", "__comet_req": "15"},
    }


def is_http_recipe(recipe) -> bool:
    # Không xét "transport": recipe tạo lúc còn gửi bằng requests vẫn là recipe HTTP, chỉ khác thư viện gửi.
    return recipe.get("recipe_source") == SOURCE


def profile(recipe) -> dict:
    from social_crawler.spiders.common.comet_graphql_client import SessionExpiredError
    from social_crawler.spiders.facebook.auth.identity import BrowserIdentityError, transport_profile

    cookies = recipe.get("cookies") or {}
    if not cookies.get("c_user", "").isdigit() or not cookies.get("xs"):
        raise SessionExpiredError("HTTP fallback requires the existing session and its recorded User-Agent")
    try:
        return transport_profile(recipe.get("headers") or {})
    except BrowserIdentityError as exc:
        raise SessionExpiredError(str(exc)) from exc


def check_response(client, response) -> None:
    from social_crawler.spiders.common.comet_graphql_client import (
        AccountRestrictedError,
        GraphQLResponseError,
        RateLimitedError,
        SessionExpiredError,
        _is_checkpoint_response,
        _parse_graphql_response,
    )
    from social_crawler.spiders.facebook.auth.http_refresh import check_security_errors, restriction_signal

    client._check_session_headers(response)
    check_security_errors(client, response)
    if response.status_code == 401:
        client._drop_rejected_session("HTTP 401 during HTTP discovery")
        raise SessionExpiredError("Facebook rejected this session during HTTP discovery")
    # Redirect không phải hạn chế với GET HTML: Facebook chuyển trang giữa các host, còn
    # redirect tới checkpoint hay logout đã được _check_session_headers xử lý.
    if response.status_code == 429 or response.status_code == 403:
        client._rest_restricted_account(f"discovery_http_{response.status_code}")
        if response.status_code == 429:
            raise RateLimitedError("Facebook rate-limited discovery; no fallback retry")
        raise AccountRestrictedError("Facebook denied HTTP discovery; no fallback retry")
    if response.status_code != 200:
        raise GraphQLResponseError(f"HTTP discovery failed with status {response.status_code}")
    if len(getattr(response, "content", response.text.encode("utf-8"))) > MAX_BYTES:
        raise GraphQLResponseError("HTTP discovery response exceeds size limit")
    if response.text.lstrip().startswith(("{", "for (;;);")):
        parsed = _parse_graphql_response(response.text)
        if not isinstance(parsed, dict):
            raise GraphQLResponseError("Unexpected HTTP discovery payload")
        if _is_checkpoint_response(parsed):
            client._raise_checkpoint(parsed)
        if str(parsed.get("error")) in {"1357001", "1357004"}:
            client._drop_rejected_session("Explicit logged-out error during HTTP discovery")
            raise SessionExpiredError("Facebook logged this session out during discovery")
        if signal := restriction_signal(parsed):
            client._rest_restricted_account(signal)
            raise AccountRestrictedError("Facebook restricted discovery; requests stopped")
        if parsed.get("errors") or parsed.get("error"):
            raise GraphQLResponseError("Facebook returned an error during HTTP discovery")


def raise_network_stop(client, exc):
    """GET thiết lập lỗi là lỗi proxy: job được xếp lại, tài khoản không bị đổ lỗi."""
    from social_crawler.runtime.pool import ProxyPoolExhaustedError
    from social_crawler.spiders.common.comet_graphql_client import NetworkError

    proxy_url = client._proxy_cfg.get("url") if client._proxy_cfg else None
    raise NetworkError(f"HTTP setup request failed ({type(exc).__name__})") from ProxyPoolExhaustedError(
        f"facebook proxy {proxy_url} unreachable during HTTP setup"
    )


def discover_authenticated(client, names: set[str], *, force: bool = False) -> dict[str, str]:
    """Dùng tài khoản và proxy đang thuê; không bao giờ đưa ID có đăng nhập vào registry công khai."""
    key = DOCS_KEY.format(account=client._account)
    known = (client._redis.get(key) or {}).get("operations", {})
    if not enabled() or not force and names <= known.keys():
        return known
    client._ensure_lease()
    scope = hashlib.sha256("|".join(sorted(names)).encode()).hexdigest()[:16]
    if not client._redis.set_nx(f"{key}:cooldown:{scope}", 1, ttl_seconds=1800):
        return known
    sources = templates()
    pages = list(
        dict.fromkeys(
            cls.REFERENCE_PAGE_URL
            for cls in sources.values()
            if cls.OPERATION_NAME.removesuffix("_facebookRelayOperation") in names
        )
    )
    if not pages:
        return known
    proxy = client._proxy_cfg
    from social_crawler.runtime.facebook_pacing import PacedSession
    from social_crawler.runtime.pool import build_proxy_url
    from social_crawler.spiders.common.comet_graphql_client import TRANSPORT_ERRORS

    # Không bao giờ đưa phiên/hũ cookie của tài khoản cho CDN, kể cả qua redirect.
    proxies = {"http": build_proxy_url(proxy), "https": build_proxy_url(proxy)} if proxy else None
    with requests.Session(
        impersonate=client._profile["target"],
        proxies=proxies,
        trust_env=False,
        curl_options={
            CurlOpt.LOW_SPEED_LIMIT: client.STALL_MIN_BYTES_PER_SECOND,
            CurlOpt.LOW_SPEED_TIME: client.STALL_SECONDS,
        },
    ) as cdn:
        assets = PacedSession(cdn, client._mark_request_finished)

        def fetch(url, asset, deadline):
            if not allowed_url(url, asset=asset):
                raise ValueError("Discovery URL outside allowed hosts")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("HTTP discovery deadline reached")
            client._throttle_asset() if asset else client._throttle()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("HTTP discovery deadline reached during throttle")
            session = assets if asset else client._session
            kwargs = {} if asset else {"cookies": client._cache["cookies"]}
            headers = client._html_headers()
            if asset:
                headers.update(ASSET_HEADERS)
            try:
                response = session.get(
                    url,
                    headers=headers,
                    allow_redirects=False,
                    timeout=min(20, remaining),
                    **kwargs,
                )
            except TRANSPORT_ERRORS as exc:
                if asset:
                    raise SourceUnavailable("Bundle request failed") from exc
                raise_network_stop(client, exc)
            if asset:
                # Request CDN không mang phiên tài khoản, nên mã trạng thái không nói lên gì về tài khoản.
                if response.status_code != 200 or len(response.content) > MAX_BYTES:
                    logger.warning("facebook_discovery_asset_unavailable", status=response.status_code)
                    raise SourceUnavailable("Bundle unavailable")
                return response.text
            client._check_session_headers(response)
            if 300 <= response.status_code < 400:
                logger.warning("facebook_discovery_page_redirected", status=response.status_code)
                raise SourceUnavailable("Discovery page redirected")
            check_response(client, response)
            return response.text

        found = discover(fetch, pages, names)
    combined = {**known, **found}
    client._redis.set(key, {"operations": combined, "captured_at": time.time()}, ttl_seconds=86400)
    return combined


def ensure_recipe(client, feature: str) -> bool:
    """Chỉ điền recipe còn thiếu. Recipe đã bắt và lỗi schema không bị thay."""
    key = FEATURE_KEYS[feature].format(account=client._account)
    existing = client._redis.get(key)
    if existing:
        if feature not in {"search", "comments", "replies"}:
            return True
        if feature in {"search", "comments"} and existing.get("pagination"):
            return True
        if feature == "replies" and (
            existing.get("reply_operations", {}).get("Depth1CommentsListPaginationQuery")
            or (existing.get("pagination") or existing).get("fb_api_req_friendly_name")
            == "Depth1CommentsListPaginationQuery"
        ):
            return True
    if not enabled():
        return False
    from social_crawler.spiders.facebook.auth.http_refresh import refresh, save_tokens

    if not client._cache.get("body_static", {}).get("fb_dtsg") and not refresh(client):
        return False
    classes = templates()
    cls = classes[feature]
    name = cls.OPERATION_NAME.removesuffix("_facebookRelayOperation")
    names = {name}
    if feature == "replies":
        names.add(classes["depth2"].OPERATION_NAME.removesuffix("_facebookRelayOperation"))
    docs = discover_authenticated(client, names)
    if not docs.get(name):
        logger.warning("facebook_http_recipe_unavailable", feature=feature, account=client._account)
        return False
    # GET dò có thể gia hạn cookie phiên. Giữ lại trước khi lưu recipe.
    save_tokens(client, {})

    def recipe_for(query_class):
        friendly = query_class.OPERATION_NAME.removesuffix("_facebookRelayOperation")
        variables = copy.deepcopy(query_class.VARIABLES)
        if feature == "search":
            variables["args"]["experience"]["type"] = "GLOBAL_SEARCH"
            variables["args"]["context"] = {"bsid": str(uuid.uuid4()), "tsid": None}
        return {"doc_id": docs[friendly], "fb_api_req_friendly_name": friendly, "variables_template": variables}

    recipe = client._redis.get(key) if existing else {**copy.deepcopy(client._cache), **recipe_for(cls)}
    recipe["pagination"] = recipe_for(cls)
    if feature == "replies":
        recipe["reply_operations"] = {
            **recipe.get("reply_operations", {}),
            **{
                n: recipe_for(c)
                for c in (cls, classes["depth2"])
                if (n := c.OPERATION_NAME.removesuffix("_facebookRelayOperation")) in docs
            },
        }
    client._ensure_lease()
    client._redis.set(key, recipe, ttl_seconds=RECIPE_TTL)
    if feature == "search":
        client._cache = recipe
        if is_http_recipe(recipe):
            from social_crawler.spiders.facebook.auth.identity import IDENTITY_KEY

            client._redis.set(
                IDENTITY_KEY.format(account=client._account),
                {"user_agent": recipe["headers"]["user-agent"], "session_user_id": recipe["cookies"]["c_user"]},
            )
    logger.info("facebook_http_recipe_ready", account=client._account, feature=feature)
    return True


def main():
    import argparse

    from social_crawler.runtime import account_lease
    from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient

    parser = argparse.ArgumentParser(description="Set up Facebook HTTP recipes from an existing session; no browser")
    parser.add_argument("--account", required=True, help="Account UID or email already stored in spider-hub")
    parser.add_argument("--feature", choices=tuple(FEATURE_KEYS), default="search")
    args = parser.parse_args()
    client = None
    try:
        client = FacebookGraphQLClient(account=args.account)
        if not ensure_recipe(client, args.feature):
            parser.exit(1, "Không tìm được mẫu truy vấn; kiểm tra phiên và metadata User-Agent đã lưu.\n")
        print(f"Đã có mẫu HTTP cho {args.feature}. Chưa gửi truy vấn crawl hoặc dữ liệu vào Kafka.")
    except Exception as exc:  # noqa: BLE001 - never expose session/proxy credentials through exception text
        parser.exit(1, f"Thiết lập HTTP dừng: {type(exc).__name__}.\n")
    finally:
        if client is not None:
            client._session.close()
            account_lease.release("facebook", client._account)


if __name__ == "__main__":
    main()
