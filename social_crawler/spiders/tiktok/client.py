"""
Plain HTTP client (no browser) for TikTok's signed endpoints. Unlike
Facebook/Threads, there is no bootstrap-via-browser step here at all - see
constants/tiktok.py's module docstring for why a browser is never touched
after the account's identity (cookie/device_id/odin_id) has been captured
once from a real, already-trusted browser session. Every request after
that - including every paginated page - is signed fresh, locally, right
here (see signature/gnarly.py), no caching of a doc_id or token needed.

TikTokClient carries everything that doesn't depend on which endpoint is
being called (identity/proxy loading, throttling, signing, retry/backoff) -
a new feature subclasses it and adds just its own methods, the way
TikTokHashtagClient does below. See _request()'s docstring for the one
thing every subclass method still owns itself.

A keyword-search client was attempted here too (a second signature,
X-Dynosaur, alongside X-Gnarly) but never got past an empty response no
matter how the request was built - byte-for-byte replays of real, freshly
captured browser requests (matching cookies/params/signatures exactly)
still came back empty through curl_cffi, which points at a TLS/HTTP2
fingerprint mismatch curl_cffi's Chrome impersonation doesn't clear for
this specific endpoint, not a logic bug. Removed rather than left half-
working; the hashtag endpoint below has no such extra protection and needs
none of this."""

from __future__ import annotations

import random
import time
from typing import Any
from urllib.parse import urlencode

from curl_cffi import requests as curl_requests

from social_crawler.constants.tiktok import (
    HASHTAG_DETAIL_URL,
    HASHTAG_ITEM_LIST_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    STATIC_PARAMS,
    STATIC_UA,
    STATIC_X_BOGUS,
)
from social_crawler.logger import get_logger
from social_crawler.services.db import get_proxy
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.facebook.auth.cookies import parse_cookie_header
from social_crawler.spiders.tiktok.auth.accounts import next_account
from social_crawler.spiders.tiktok.signature.gnarly import get_X_Gnarly

logger = get_logger(__name__)


class TikTokBlockedError(RuntimeError):
    """TikTok returned an empty/rejected response - the account's cookie
    (ttwid/msToken/verifyFp) has likely gone stale, or its device_id/odin_id
    lost trust. Re-capture the account's identity from a real browser
    session and update its platform_accounts row (platform='tiktok')."""


class TikTokRateLimitedError(RuntimeError):
    """TikTok is rate-limiting this identity/IP even after retrying with
    backoff. Not a dead identity - re-capturing won't help, back off and
    retry later instead."""


class TikTokNetworkError(RuntimeError):
    """Every retry failed to even get an HTTP response back (proxy down,
    DNS failure, TLS handshake failure, timeout) - TikTok never actually
    saw this request, so the account's identity is not the problem. Check
    connectivity to the configured platform_proxies row (platform='tiktok')
    instead of re-capturing cookie/device_id/odin_id."""


class TikTokClient:
    """Identity/session/signing/retry machinery shared by every TikTok
    endpoint client - nothing here is hashtag-specific. Subclasses add
    endpoint methods that call self._request(...); see TikTokHashtagClient
    below for the shape."""

    def __init__(self, redis_cache: RedisCache | None = None):
        self._redis = redis_cache or RedisCache()
        account = next_account(self._redis)
        if account is None:
            raise RuntimeError(
                "No enabled tiktok row in platform_accounts. Capture cookie/device_id/odin_id "
                "from a real browser session's DevTools first - see this module's docstring "
                "(account_id -> device_id, token -> odin_id, cookie -> raw Cookie header)."
            )

        cookies = parse_cookie_header(account["cookie"])
        missing = [name for name in ("ttwid",) if name not in cookies]
        if missing:
            raise RuntimeError(
                f"tiktok platform_accounts row is missing required cookie(s) {missing} - "
                "re-capture from a real browser session."
            )
        self._cookies = cookies
        self._ms_token = cookies.get("msToken", "")
        self._verify_fp = cookies.get("s_v_web_id", "")
        self._device_id = account["id"]
        self._odin_id = account["token"]

        proxy = None
        proxy_cfg = get_proxy("tiktok")
        if proxy_cfg:
            proxy = {
                "http": f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}",
                "https": f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}",
            }
        logger.info("tiktok_session_ready", device_id=self._device_id, proxy=proxy_cfg["url"] if proxy_cfg else None)

        self._session = curl_requests.Session(impersonate="chrome", proxies=proxy)
        self._last_request_at: float | None = None

    def _throttle(self) -> None:
        if self._last_request_at is not None:
            target_gap = MIN_REQUEST_INTERVAL_SECONDS + random.uniform(0, REQUEST_INTERVAL_JITTER_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.time()

    def _request(self, endpoint: str, extra_params: dict[str, str], referer: str) -> dict[str, Any]:
        """Signs and sends one GET to `endpoint`. `referer` is the full
        URL a real browser would have been on when firing this request -
        e.g. a hashtag page (`/tag/<name>`) or a search results page
        (`/search?q=<query>`) - subclass methods build this themselves
        since it's the one thing that actually varies by endpoint; nothing
        else here has to change to add a new one."""
        params = {
            **STATIC_PARAMS,
            **extra_params,
            "WebIdLastTime": str(int(time.time())),
            "device_id": self._device_id,
            "odinId": self._odin_id,
            "referer": referer,
            "root_referer": referer,
            "verifyFp": self._verify_fp,
            "msToken": self._ms_token,
        }

        query_string = urlencode(params)
        gnarly = get_X_Gnarly(query_string, "", STATIC_UA)
        params["X-Bogus"] = STATIC_X_BOGUS
        params["X-Gnarly"] = gnarly

        url = f"{endpoint}?{urlencode(params)}"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,vi;q=0.8",
            "referer": referer,
            "user-agent": STATIC_UA,
        }

        logger.info("sending_request", endpoint=endpoint, **extra_params)
        self._throttle()
        resp = self._post_with_retry(url, headers)

        if len(resp.content) == 0:
            from social_crawler.services.db import disable_account

            key = f"tiktok_block_streak:{self._device_id}"
            streak = self._redis.incr(key)
            self._redis.expire(key, 1800)

            if streak >= 3:
                disabled = disable_account("tiktok", self._device_id, reason="repeated empty response (likely stale identity)")
                logger.error("tiktok_account_disabled_repeated_block", telegram=True, device_id=self._device_id, disabled=disabled)
            raise TikTokBlockedError(
                f"TikTok returned an empty response (status={resp.status_code}). The account's "
                "identity has likely gone stale - re-capture cookie/device_id/odin_id."
            )

        return resp.json()

    def _post_with_retry(self, url: str, headers: dict[str, str]):
        last_exc: Exception | None = None
        resp = None
        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, headers=headers, cookies=self._cookies, timeout=15)
            except curl_requests.RequestsError as exc:
                last_exc = exc
                logger.warning("request_failed", attempt=attempt, max_retries=MAX_RETRIES, error=str(exc))
            else:
                if resp.status_code in TRANSIENT_STATUS_CODES:
                    logger.warning(
                        "tiktok_returned_error_status",
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=MAX_RETRIES,
                    )
                else:
                    return resp

            if attempt < MAX_RETRIES:
                # Jitter on top of the exponential base - same rationale
                # as facebook/auth/graphql_client.py's own retry jitter.
                delay = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, RETRY_BACKOFF_JITTER_SECONDS)
                time.sleep(delay)

        if resp is not None and resp.status_code == 429:
            raise TikTokRateLimitedError(
                f"TikTok rate-limited this request (status=429) even after {MAX_RETRIES} retries with backoff."
            )
        if resp is not None:
            return resp
        # resp is still None here - every attempt raised RequestsError
        # (connection-level failure), never even reached TikTok's server,
        # so this is a network/proxy problem, not a stale identity.
        raise TikTokNetworkError(f"Request failed after {MAX_RETRIES} attempts: {last_exc}") from last_exc


class TikTokHashtagClient(TikTokClient):
    def resolve_hashtag(self, name: str) -> str | None:
        """A hashtag's numeric TikTok id, given its name (no leading '#',
        no spaces - e.g. "holinhtrangsi"). None if TikTok has no such
        hashtag. This id is what search_hashtag()'s `challenge_id` wants -
        it doesn't change, so callers can resolve once and reuse it for
        every subsequent search_hashtag()/pagination call."""
        name = name.lstrip("#").strip()
        if not name.isascii() or " " in name:
            raise ValueError(
                f"{name!r} isn't a TikTok hashtag slug - pass the actual tag "
                "(no spaces/diacritics, e.g. 'holinhtrangsi'), not a movie "
                "title or display keyword. curl_cffi can't put non-ASCII "
                "text in a header, and TikTok's real hashtag ids look "
                "nothing like a Vietnamese title anyway."
            )
        data = self._request(HASHTAG_DETAIL_URL, {"challengeName": name}, referer=f"https://www.tiktok.com/tag/{name}")
        return (data.get("challengeInfo") or {}).get("challenge", {}).get("id")

    def search_hashtag(self, challenge_id: str, cursor: int = 0, count: int = 30) -> dict[str, Any]:
        """Fetch one page of a hashtag's videos (the first page when cursor
        is 0). `challenge_id` is TikTok's numeric hashtag id - not the
        hashtag name itself (see resolve_hashtag)."""
        return self._request(
            HASHTAG_ITEM_LIST_URL,
            {"challengeID": challenge_id, "count": str(count), "cursor": str(cursor)},
            referer=f"https://www.tiktok.com/tag/{challenge_id}",
        )
