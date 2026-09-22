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
thing every subclass method still owns itself."""

from __future__ import annotations

import random
import time
from typing import Any
from urllib.parse import urlencode

from curl_cffi import requests as curl_requests

from social_crawler.constants.tiktok import (
    ADAPTIVE_INTERVAL_MAX_SECONDS,
    COMMENT_ITEM_LIST_URL,
    COMMENT_REPLY_LIST_URL,
    CURL_CFFI_IMPERSONATE_TARGET,
    CURL_CFFI_UA,
    HASHTAG_DETAIL_URL,
    HASHTAG_ITEM_LIST_URL,
    MAX_RETRIES,
    MIN_REQUEST_INTERVAL_SECONDS,
    REQUEST_INTERVAL_JITTER_SECONDS,
    RETRY_BACKOFF_BASE_SECONDS,
    RETRY_BACKOFF_JITTER_SECONDS,
    SIGNED_QUERY_PARAM_ORDER,
    STATIC_PARAMS,
    STATIC_X_BOGUS,
    THROTTLE_REDIS_KEY_TMPL,
)
from social_crawler.logger import get_logger
from social_crawler.services import pool, proxy_provider
from social_crawler.services.db import platform_has_any_proxy
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.tiktok.auth.accounts import is_logged_in_cookie, next_account
from social_crawler.spiders.tiktok.auth.cookies import cookie_map
from social_crawler.spiders.tiktok.signature.dynosaur import get_X_Dynosaur
from social_crawler.spiders.tiktok.signature.gnarly import get_X_Gnarly

logger = get_logger(__name__)

# A synthetic identity's device_id/odinId only need to look like TikTok's own
# (large numeric ids, same digit count as e.g. "7685251565930628616") -
# confirmed by direct live testing (2026-09-17) that guest-mode item_list
# doesn't validate these against any server-side registry, just that a
# request's X-Gnarly matches its own query string. A range starting with a
# plausible leading digit is cosmetic, not load-bearing.
_SYNTHETIC_ID_MIN = 10**18
_SYNTHETIC_ID_MAX = 10**19 - 1


def _generate_synthetic_id() -> str:
    return str(random.randint(_SYNTHETIC_ID_MIN, _SYNTHETIC_ID_MAX))


# A dedicated proxiestrust.com rotating-slot plan (US exit IPs), separate
# from services/proxy_provider.py's own default PROXIESTRUST_API_TOKEN (a
# different, VN-purposed plan pool.py's circuit breaker refreshes) - see
# that module's own docstring for why these must never share one token.
# Minting a fresh lease per synthetic client (see __init__ below) rather
# than storing one in platform_proxies: this plan's leases expire after
# ~15-20 minutes (its own time_seconds_to_die), so a static DB row would
# silently start 407ing once that lease lapses.
_PROXIESTRUST_TIKTOK_TOKEN_ENV_VAR = "PROXIESTRUST_TIKTOK_US_API_TOKEN"

# AIMD-style adjustment for the adaptive per-device throttle interval (see
# TikTokClient._adjust_interval) - same growth/decay shape as Facebook/
# Threads' own comet_graphql_client.py: grow fast on any sign of stress
# (one retry is enough to react to), decay slowly so a single clean
# request right after a rough patch doesn't immediately erase the caution.
_ADAPTIVE_INTERVAL_GROWTH_FACTOR = 1.7
_ADAPTIVE_INTERVAL_DECAY_FACTOR = 0.85
# How long a raised interval survives with no new stress signal before
# _current_interval falls back to reading MIN_REQUEST_INTERVAL_SECONDS
# again - a device that had a rough 10 minutes an hour ago shouldn't still
# be throttled extra-cautiously now.
_ADAPTIVE_INTERVAL_TTL_SECONDS = 1800


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

    def __init__(
        self,
        redis_cache: RedisCache | None = None,
        *,
        exclude_ids: set[str] | None = None,
        require_login: bool = False,
        force_guest: bool = False,
        synthetic: bool = False,
    ):
        self._redis = redis_cache or RedisCache()
        self._synthetic = synthetic
        # Only ever set to a real platform_proxies ProxyRow (never the
        # synthetic branch's ephemeral vendor lease - see that branch's own
        # comment) - _record_proxy_outcome_once below is a no-op while this
        # stays None, exactly like comet_graphql_client.py's own pattern.
        self._proxy_cfg: dict[str, Any] | None = None
        self._proxy_outcome_recorded = False

        if synthetic:
            # Invariant this whole branch exists to hold: device_id/odinId,
            # proxy IP, and the ttwid/csrf/chain cookie set below must all
            # be minted together, once, for this one client instance - never
            # a new identity kept on an old IP, or an old identity moved to
            # a new IP mid-session (see this module's own docstring for why
            # a *reused* identity is what earns TikTok's suspicion in the
            # first place; an inconsistent identity/IP pairing is the same
            # kind of anomaly signal). A retry means a whole new
            # TikTokClient(synthetic=True) - never patching just one of
            # these three onto an existing client.
            #
            # Guest-only identity minted fresh for this one client instance,
            # no platform_accounts row involved at all - confirmed by direct
            # live A/B testing (2026-09-17) that a brand-new device_id/
            # odinId/cookie set, never touched by a browser, gets full-trust
            # guest access identical to a real captured account, as long as
            # X-Gnarly is signed correctly. The failure mode this sidesteps:
            # a *reused* device_id accumulates TikTok's own abuse signal
            # from repeated automated traffic (confirmed live the same day -
            # three different long-lived platform_accounts rows, three
            # different proxies, all started needing a real X-Dynosaur this
            # project has no working local implementation of, while a
            # same-session fresh identity needed none) - so the fix isn't a
            # better signature, it's never reusing an identity long enough
            # to earn that suspicion in the first place. See
            # hashtag_search/search.py's own module docstring for the fuller
            # investigation trail.
            self._device_id = _generate_synthetic_id()
            self._odin_id = _generate_synthetic_id()
            self._cookies: dict[str, str] = {}
            self._ms_token = ""
            self._verify_fp = ""
            self._is_logged_in = False
            account_email = None

            # A fresh IP lease paired with the fresh identity above - see
            # _PROXIESTRUST_TIKTOK_TOKEN_ENV_VAR's own comment for why this
            # is its own plan/token rather than platform_proxies.
            # ip_allowlist=True: confirmed live (2026-09-17) the default
            # username:password-authenticated lease keeps cycling through
            # the same ~4 IPs, most already TikTok-blocked; the
            # IP-allowlisted lease (proxy_ip_allow) draws from a visibly
            # different, currently-clean pool - see get_new_proxy's own
            # docstring for the mechanism and its one caveat (only usable
            # by the machine that called get_new, which is exactly what
            # happens here). Falls back to the regular DB proxy pool when
            # that token isn't configured (e.g. a dev environment without
            # it), same as before synthetic identities existed.
            lease = proxy_provider.get_new_proxy(
                token_env_var=_PROXIESTRUST_TIKTOK_TOKEN_ENV_VAR, ip_allowlist=True
            )
            if lease is not None:
                proxy_cfg = {
                    "url": f"{lease['host']}:{lease['port']}",
                    "username": lease["username"],
                    "password": lease["password"],
                }
                # Not a platform_proxies row (self._proxy_cfg stays None,
                # below) - a fresh vendor-API lease, one-off and never
                # reused, so there's no cooldown to track: a bad lease just
                # means the next call to get_new_proxy mints a different
                # one, not "wait for this IP to recover".
            else:
                proxy_cfg = pool.acquire_proxy("tiktok")
                self._proxy_cfg = proxy_cfg
                if proxy_cfg is None and platform_has_any_proxy("tiktok"):
                    # Same "never run steady-state crawl traffic unproxied"
                    # rule as the non-synthetic path below - see
                    # ProxyPoolExhaustedError's own docstring.
                    raise TikTokNetworkError(
                        "tiktok: no usable proxy available for a synthetic guest identity."
                    ) from pool.ProxyPoolExhaustedError(
                        "tiktok: no usable proxy available for a synthetic guest identity."
                    )
        else:
            account = next_account(self._redis, exclude_ids=exclude_ids, require_login=require_login)
            if account is None:
                if require_login:
                    raise RuntimeError(
                        "No enabled logged-in tiktok account (cookie needs sessionid). "
                        "Paste a logged-in Cookie header on the TikTok tab, then Try saved session."
                    )
                raise RuntimeError(
                    "No enabled tiktok row in platform_accounts. Capture cookie/device_id/odin_id "
                    "from a real browser session's DevTools first - see this module's docstring "
                    "(account_id -> device_id, token -> odin_id, cookie -> raw Cookie header)."
                )

            cookies = cookie_map(account["cookie"])
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
            account_email = account.get("email") or None
            # sessionid is TikTok web's real logged-in session cookie - present
            # only when this account's cookie field was captured from (or had
            # pasted into it) an actual logged-in browser session, not just a
            # guest visit. Confirmed by direct testing to return meaningfully
            # more results per hashtag than a guest-only identity when the
            # request is signed by a real browser - see _request()'s
            # user_is_login param below.
            #
            # force_guest overrides this to False regardless of the cookie's
            # actual login state - this client (TikTokHashtagClient/comment
            # replay) signs locally with gnarly.py, which only produces a
            # signature TikTok accepts for a *guest* request (user_is_login=
            # false); a logged-in one additionally needs a real X-Dynosaur
            # header only TikTok's own JS can compute (confirmed by direct
            # testing - no local implementation of it exists in this project,
            # see constants/tiktok.py's own POST_ITEM_LIST_URL comment for the
            # fuller investigation trail), so a
            # locally-signed request claiming to be logged in just gets an
            # empty response even with a perfectly valid sessionid cookie.
            # Every currently-enabled tiktok account happens to carry a real
            # sessionid (captured for the browser-driven login experiment) -
            # without this override, this client would have no usable account
            # left to rotate to at all.
            self._is_logged_in = False if force_guest else is_logged_in_cookie(account["cookie"])

            try:
                proxy_cfg = pool.acquire_proxy_for_account("tiktok", account["id"], required=True)
                self._proxy_cfg = proxy_cfg
            except pool.ProxyPoolExhaustedError as exc:
                # required=True: this is steady-state crawl traffic - never fall
                # back to running unproxied (see ProxyPoolExhaustedError's own
                # docstring). Re-raised as TikTokNetworkError so it flows through
                # the same retry/Telegram-alert handling every TikTok spider
                # already has for "proxy down" (see e.g. hashtag_search/search.py's
                # `except TikTokNetworkError`).
                raise TikTokNetworkError(str(exc)) from exc

        proxy = None
        if proxy_cfg:
            # An IP-allowlisted synthetic lease (see the ip_allowlist branch
            # above) carries no username/password at all - authorization is
            # by source IP, not credentials - so building a user:pass@ URL
            # for one would bake the literal string "None:None@" into it.
            if proxy_cfg.get("username") and proxy_cfg.get("password"):
                proxy_url = f"http://{proxy_cfg['username']}:{proxy_cfg['password']}@{proxy_cfg['url']}"
            else:
                proxy_url = f"http://{proxy_cfg['url']}"
            proxy = {"http": proxy_url, "https": proxy_url}
        logger.info(
            "tiktok_session_ready",
            device_id=self._device_id,
            email=account_email,
            proxy=proxy_cfg["url"] if proxy_cfg else None,
            logged_in=self._is_logged_in,
            synthetic=synthetic,
        )

        # impersonate=CURL_CFFI_IMPERSONATE_TARGET, not the bare "chrome"
        # alias - see CURL_CFFI_UA's own docstring for why the two must
        # always be a matched pair (a live-confirmed bug, not caution for
        # its own sake: bare "chrome" plus a UA claiming a Chrome version
        # curl_cffi has no TLS fingerprint for was getting an empty
        # response on literally every request, regardless of identity/IP).
        self._session = curl_requests.Session(impersonate=CURL_CFFI_IMPERSONATE_TARGET, proxies=proxy)
        self._last_request_at: float | None = None

        if synthetic:
            # Mints ttwid/tt_csrf_token/tt_chain_token via a plain GET's own
            # Set-Cookie headers - confirmed live (2026-09-17) this needs no
            # JS at all, curl_cffi's Chrome TLS impersonation is enough.
            # self._session (a curl_cffi Session, not a one-off request)
            # keeps these for every subsequent call this client makes.
            try:
                mint_resp = self._session.get(
                    "https://www.tiktok.com/",
                    headers={"user-agent": CURL_CFFI_UA},
                    timeout=15,
                )
            except curl_requests.RequestsError as exc:
                raise TikTokNetworkError(f"Failed to mint a synthetic guest session: {exc}") from exc
            # Pulled into a plain dict (rather than left implicit in the
            # session's own jar) so every call below stays explicit about
            # what it's sending, same as the non-synthetic path's self._cookies.
            self._cookies = dict(mint_resp.cookies)

    def _record_proxy_outcome_once(self, *, success: bool) -> None:
        """Feeds pool.py's circuit breaker (cooldown_until/consecutive_
        failures on the platform_proxies row) so a proxy that keeps failing
        actually gets cooled down instead of being handed out again on the
        next acquire_proxy_for_account call - this client used to acquire a
        proxy and never report back what happened with it at all, so the
        breaker never engaged for TikTok's own traffic (Facebook/Threads'
        comet_graphql_client.py already does this - see its own
        _record_proxy_outcome_once). Same "only the first call this
        client's lifetime counts" guard: a request that retries 3 times
        internally must not record 3 separate outcomes for one logical
        call. self._proxy_cfg is None for the synthetic-lease path (nothing
        to release - see its own comment in __init__), so this is a no-op
        there by construction."""
        if self._proxy_cfg is None or self._proxy_outcome_recorded:
            return
        self._proxy_outcome_recorded = True
        pool.release_proxy(self._proxy_cfg, success=success)

    def _throttle_key(self) -> str:
        return THROTTLE_REDIS_KEY_TMPL.format(device_id=self._device_id)

    def _current_interval(self) -> float:
        """The base pacing interval to use right now - MIN_REQUEST_INTERVAL_SECONDS
        normally, or a higher Redis-persisted value if this device has hit
        429/5xx/network errors recently (see _adjust_interval). Persisted
        (not just in-memory) because a `scrapy crawl` run is a short-lived
        subprocess - without Redis, a run that got throttled right before
        exiting would teach the next run nothing."""
        stored = self._redis.get(self._throttle_key())
        if stored is None:
            return MIN_REQUEST_INTERVAL_SECONDS
        return max(MIN_REQUEST_INTERVAL_SECONDS, float(stored))

    def _adjust_interval(self, *, stressed: bool) -> None:
        """Called after every request settles: grows the persisted intervala
        on any 429/5xx/network signal, decays it back down on a clean
        response with no prior signal this call. See the module-level
        _ADAPTIVE_INTERVAL_* constants for the growth/decay factors and
        TTL. Deliberately not fed by a TikTokBlockedError (empty-body)
        response - that's an identity-trust signal (see that error's own
        docstring), not a pacing one, and slowing down further wouldn't
        make a stale device_id/cookie valid again."""
        key = self._throttle_key()
        stored = self._redis.get(key)
        if stored is None:
            if not stressed:
                return  # already at baseline - nothing to persist
            current = MIN_REQUEST_INTERVAL_SECONDS
        else:
            current = max(MIN_REQUEST_INTERVAL_SECONDS, float(stored))

        if stressed:
            new_interval = min(ADAPTIVE_INTERVAL_MAX_SECONDS, current * _ADAPTIVE_INTERVAL_GROWTH_FACTOR)
        else:
            new_interval = max(MIN_REQUEST_INTERVAL_SECONDS, current * _ADAPTIVE_INTERVAL_DECAY_FACTOR)

        if new_interval <= MIN_REQUEST_INTERVAL_SECONDS:
            self._redis.delete(key)
            return

        logger.info(
            "adaptive_interval_adjusted",
            device_id=self._device_id,
            stressed=stressed,
            interval_seconds=round(new_interval, 2),
        )
        self._redis.set(key, new_interval, ttl_seconds=_ADAPTIVE_INTERVAL_TTL_SECONDS)

    def _throttle(self) -> None:
        base_interval = self._current_interval()
        if self._last_request_at is not None:
            target_gap = base_interval + random.uniform(0, REQUEST_INTERVAL_JITTER_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.info(
                    "throttling", delay_seconds=round(remaining, 2), base_interval_seconds=round(base_interval, 2)
                )
                time.sleep(remaining)
        self._last_request_at = time.time()

    def _ordered_query_pairs(self, params: dict[str, str]) -> list[tuple[str, str]]:
        """Emit (key, value) pairs in SIGNED_QUERY_PARAM_ORDER. TikTok's
        challenge item_list verifier is order-sensitive for the signed
        query string — see that constant's own comment. Unknown keys keep
        their relative insertion order after the known prefix."""
        pairs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for key in SIGNED_QUERY_PARAM_ORDER:
            if key in params:
                pairs.append((key, params[key]))
                seen.add(key)
        for key, value in params.items():
            if key not in seen:
                pairs.append((key, value))
        return pairs

    def _request(
        self,
        endpoint: str,
        extra_params: dict[str, str],
        referer: str,
        *,
        sign_dynosaur: bool = False,
    ) -> dict[str, Any]:
        """Signs and sends one GET to `endpoint`. `referer` is the full
        URL a real browser would have been on when firing this request -
        e.g. a hashtag page (`/tag/<name>`) or a search results page
        (`/search?q=<query>`) - subclass methods build this themselves
        since it's the one thing that actually varies by endpoint; nothing
        else here has to change to add a new one.

        `sign_dynosaur`: /api/comment/list/ rejects Gnarly-only requests
        with HTTP 200 + empty body; local get_X_Dynosaur unlocks it
        (confirmed live A/B 2026-09-18). Hashtag item_list must keep this
        False — it works without Dynosaur and must not change shape.
        """
        # Prefer msToken freshly Set-Cookie'd by the previous response
        # (browser jar behaviour). Falling back to the mint-time value is
        # fine when none has arrived yet.
        jar_ms = ""
        try:
            jar_ms = self._session.cookies.get("msToken") or self._cookies.get("msToken") or ""
        except Exception:
            jar_ms = self._cookies.get("msToken") or ""
        if jar_ms:
            self._ms_token = jar_ms

        params = {
            **STATIC_PARAMS,
            **extra_params,
            "WebIdLastTime": str(int(time.time())),
            "device_id": self._device_id,
            "odinId": self._odin_id,
            "referer": referer,
            "root_referer": referer,
            "msToken": self._ms_token,
            "user_is_login": "true" if self._is_logged_in else "false",
        }
        # Confirmed live 2026-09-17: an empty verifyFp= query param (what
        # synthetic guests always produced) turns a working request into
        # HTTP 200 + empty body. Real browser guests omit the key entirely
        # when they have no fingerprint — only send it when non-empty.
        if self._verify_fp:
            params["verifyFp"] = self._verify_fp

        # Order before signing — urlencode(dict) would use insertion order
        # from the merge above, which is exactly the "prod_client_order"
        # shape that A/B'd empty against an otherwise-identical browser
        # capture (see SIGNED_QUERY_PARAM_ORDER).
        ordered = self._ordered_query_pairs(params)
        query_string = urlencode(ordered)
        gnarly = get_X_Gnarly(query_string, "", CURL_CFFI_UA)
        ordered.append(("X-Bogus", STATIC_X_BOGUS))
        ordered.append(("X-Gnarly", gnarly))
        if sign_dynosaur:
            # Sign over the same base query Gnarly used (pre Bogus/Gnarly).
            ordered.append(("X-Dynosaur", get_X_Dynosaur(query_string, CURL_CFFI_UA, "")))

        url = f"{endpoint}?{urlencode(ordered)}"
        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9,vi;q=0.8",
            "referer": referer,
            "user-agent": CURL_CFFI_UA,
        }

        logger.info(
            "sending_request",
            endpoint=endpoint,
            logged_in=self._is_logged_in,
            cookie_count=len(self._cookies),
            cookie_names=sorted(self._cookies),
            referer=referer,
            sign_dynosaur=sign_dynosaur,
            **extra_params,
        )
        self._throttle()
        resp = self._post_with_retry(url, headers)

        # Keep identity cookies in sync with whatever TikTok rotated on
        # this response (especially msToken — every successful item_list
        # Set-Cookies a new one).
        try:
            for name, value in resp.cookies.items():
                self._cookies[name] = value
                if name == "msToken":
                    self._ms_token = value
        except Exception:
            pass

        if len(resp.content) == 0:
            if self._synthetic:
                # No platform_accounts row to protect and no point tracking
                # a block streak keyed on an id this client will never reuse
                # again - the caller's own retry (a fresh TikTokClient,
                # fresh synthetic identity) already is the fix, see
                # hashtag_search/search.py's start().
                logger.warning(
                    "tiktok_empty_response",
                    status_code=resp.status_code,
                    body_len=0,
                    endpoint=endpoint,
                    logged_in=self._is_logged_in,
                    synthetic=True,
                )
            else:
                from social_crawler.services.db import disable_account

                key = f"tiktok_block_streak:{self._device_id}"
                streak = self._redis.incr(key)
                self._redis.expire(key, 1800)
                logger.warning(
                    "tiktok_empty_response",
                    status_code=resp.status_code,
                    body_len=0,
                    streak=streak,
                    endpoint=endpoint,
                    logged_in=self._is_logged_in,
                )

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
        # True as soon as any attempt this call sees 429/5xx/a network
        # error - feeds _adjust_interval so a request that only succeeded
        # after retrying still counts as stress, not a clean response.
        stressed = False
        TRANSIENT_STATUS_CODES = {429, 500, 502, 503, 504}

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, headers=headers, cookies=self._cookies, timeout=15)
            except curl_requests.RequestsError as exc:
                last_exc = exc
                stressed = True
                logger.warning(
                    "request_failed",
                    device_id=self._device_id,
                    attempt=attempt,
                    max_retries=MAX_RETRIES,
                    error=str(exc),
                )
            else:
                if resp.status_code in TRANSIENT_STATUS_CODES:
                    stressed = True
                    logger.warning(
                        "tiktok_returned_error_status",
                        status_code=resp.status_code,
                        attempt=attempt,
                        max_retries=MAX_RETRIES,
                    )
                else:
                    self._adjust_interval(stressed=stressed)
                    self._record_proxy_outcome_once(success=not stressed)
                    return resp

            if attempt < MAX_RETRIES:
                # Jitter on top of the exponential base - same rationale
                # as facebook/auth/graphql_client.py's own retry jitter.
                delay = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, RETRY_BACKOFF_JITTER_SECONDS)
                time.sleep(delay)

        self._adjust_interval(stressed=True)
        self._record_proxy_outcome_once(success=False)

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

    def search_hashtag(self, challenge_id: str, cursor: int = 0, count: int = 30, hashtag: str = "") -> dict[str, Any]:
        """Fetch one page of a hashtag's videos (the first page when cursor
        is 0). `challenge_id` is TikTok's numeric hashtag id - not the
        hashtag name itself (see resolve_hashtag). Referer must be the
        public `/tag/<slug>` URL, matching challenge/detail - `/tag/<id>`
        is what logged-in item_list rejects with an empty 200."""
        slug = hashtag.lstrip("#").strip() or challenge_id
        return self._request(
            HASHTAG_ITEM_LIST_URL,
            {"challengeID": challenge_id, "count": str(count), "cursor": str(cursor)},
            referer=f"https://www.tiktok.com/tag/{slug}",
        )


class TikTokCommentClient(TikTokClient):
    """Guest curl_cffi client for /api/comment/list/ — same synthetic
    mint path as TikTokHashtagClient, but every request also carries a
    locally computed X-Dynosaur (see _request(sign_dynosaur=True))."""

    def warm_session(self) -> None:
        """Run hashtag detail + item_list so the jar picks up msToken before
        comment/list. Live cutover (2026-09-18): Dynosaur alone is not
        enough on a cold guest — the successful A/B always hit item_list
        first (which Set-Cookies msToken); challenge/detail alone does not.
        Failures here are ignored — list_comments still runs."""
        try:
            data = self._request(
                HASHTAG_DETAIL_URL,
                {"challengeName": "phimviet"},
                referer="https://www.tiktok.com/tag/phimviet",
            )
            challenge_id = (data.get("challengeInfo") or {}).get("challenge", {}).get("id")
            if not challenge_id:
                logger.info("tiktok_comment_warm_no_challenge", device_id=self._device_id)
                return
            self._request(
                HASHTAG_ITEM_LIST_URL,
                {"challengeID": str(challenge_id), "count": "4", "cursor": "0"},
                referer="https://www.tiktok.com/tag/phimviet",
            )
        except TikTokBlockedError:
            logger.info("tiktok_comment_warm_empty", device_id=self._device_id)

    def list_comments(
        self,
        aweme_id: str,
        *,
        cursor: int = 0,
        count: int = 20,
        video_url: str = "",
    ) -> dict[str, Any]:
        """One page of top-level comments for `aweme_id` (TikTok video id).
        `video_url` should be the public permalink used as Referer; falls
        back to a synthetic /video/<id> path when the caller only has the id.
        """
        referer = video_url.strip() or f"https://www.tiktok.com/@_/video/{aweme_id}"
        return self._request(
            COMMENT_ITEM_LIST_URL,
            {
                "aweme_id": str(aweme_id),
                "count": str(count),
                "cursor": str(cursor),
                "from_page": "video",
                "enter_from": "tiktok_web",
                "is_non_personalized": "false",
                "current_region": "VN",
            },
            referer=referer,
            sign_dynosaur=True,
        )

    def list_replies(
        self,
        *,
        comment_id: str,
        item_id: str,
        cursor: int = 0,
        count: int = 20,
        video_url: str = "",
    ) -> dict[str, Any]:
        """One page of replies under a top-level `comment_id` on video
        `item_id`. Same Dynosaur signing as list_comments; confirmed live
        2026-09-18 against /api/comment/list/reply/ (cursor starts at 0).
        """
        referer = video_url.strip() or f"https://www.tiktok.com/@_/video/{item_id}"
        return self._request(
            COMMENT_REPLY_LIST_URL,
            {
                "comment_id": str(comment_id),
                "item_id": str(item_id),
                "count": str(count),
                "cursor": str(cursor),
                "from_page": "video",
                "current_region": "VN",
            },
            referer=referer,
            sign_dynosaur=True,
        )

