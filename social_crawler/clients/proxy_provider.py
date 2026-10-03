"""Thin client for proxiestrust.com's "get new proxy" API - lets
services/pool.py request a fresh IP on an already-purchased rotating slot
once the pool's own circuit breaker (see pool.REPIN_AFTER_CONSECUTIVE_
FAILURES) decides a proxiestrust-sourced platform_proxies row is dead,
instead of just abandoning that row and re-pinning accounts elsewhere
forever (which only ever shrinks the usable pool - nothing else in this
project ever revives a dead proxy row).

Degrades to None (not an exception) when the relevant token env var isn't
set, same convention as clients/kira.py - a proxiestrust proxy going
unrefreshed should behave exactly like it did before this module existed
(abandoned, account re-pinned elsewhere), not crash whatever called into
this.

This project has more than one proxiestrust.com plan/token (different exit
countries, purchased for different platforms) - get_new_proxy()'s
provider_key picks which proxy_providers row (see services/
proxy_settings.py) to use; never default multiple call sites onto the same
provider just because they both happen to call this module. The vendor
API URL, token, ip_allowlist mode and all timing knobs below come from the
dashboard's Settings page (proxy_settings/proxy_providers), not code."""

from __future__ import annotations

import re
import time
from typing import TypedDict

import requests

from social_crawler import env  # noqa: F401 - import for its load_dotenv() side effect
from social_crawler.db.proxy_settings import get_provider, get_setting
from social_crawler.logger import get_logger

logger = get_logger(__name__)

# The vendor's own per-token cooldown between two get_new calls (observed
# 2026-09-17: ~90s; operator-confirmed minimum spacing ~60s) rejects an
# early call with statusCode 405 and a Vietnamese "còn NN giây" message
# rather than handing back the still-live previous lease. Callers that
# retry immediately would fall back to a worse proxy source instead of
# getting a fresh IP. We therefore:
#   1. Space our own calls at least provider_min_get_new_interval_seconds
#      apart (per provider) so we usually never hit the 405 at all.
#   2. If we still get a cooldown rejection, wait once (capped at
#      provider_max_cooldown_wait_seconds) and retry.
_COOLDOWN_MESSAGE_PATTERN = re.compile(r"(\d+)")

# Monotonic timestamp of the last get_new HTTP attempt per provider key —
# enforces the min interval across hashtag + comments synthetic mints
# sharing the same provider.
_last_get_new_at: dict[str, float] = {}


def _redact(text: str, token: str) -> str:
    """requests' own exception text embeds the full request URL - token
    query param included - so it must never be logged verbatim (it was, in
    proxy_health_check.log/daily_run.log, until 2026-09-28)."""
    return text.replace(token, "***") if token else text


class NewProxy(TypedDict):
    host: str
    port: int
    # None for an ip_allow_on lease - proxiestrust authorizes those by the
    # caller's own source IP (see get_new_proxy's ip_allowlist param), not
    # by credentials, so there's nothing to put in an http://user:pass@ URL.
    username: str | None
    password: str | None


def is_proxiestrust_url(proxy_url: str) -> bool:
    """Whether `proxy_url` (platform_proxies.proxy_url, "host:port") looks
    like it was issued by proxiestrust.com - the only provider this module
    knows how to refresh. Other providers' dead proxies are left exactly as
    every provider's were before this module existed (abandoned)."""
    return "proxiestrust.com" in proxy_url


def _parse_proxy_string(raw: str) -> NewProxy | None:
    """"host:port:username:password" (proxiestrust's own format, matching
    the "PORT XOAY" credentials this project was already given by hand) ->
    the pieces platform_proxies' own columns want. None if the shape
    doesn't match - logged by the caller, not raised, since a malformed
    response from a paid third-party API is exactly the kind of thing that
    must degrade to "couldn't refresh this time", not crash the circuit
    breaker that called in here."""
    parts = raw.split(":")
    if len(parts) != 4:
        return None
    host, port, username, password = parts
    if not (host and port.isdigit() and username and password):
        return None
    return {"host": host, "port": int(port), "username": username, "password": password}


def _parse_ip_allow_string(raw: str) -> NewProxy | None:
    """"host:port" (proxiestrust's own proxy_ip_allow shape - no embedded
    credentials, see get_new_proxy's ip_allowlist docstring). None if the
    shape doesn't match, same degrade-don't-raise rationale as
    _parse_proxy_string."""
    parts = raw.split(":")
    if len(parts) != 2:
        return None
    host, port = parts
    if not (host and port.isdigit()):
        return None
    return {"host": host, "port": int(port), "username": None, "password": None}


def _wait_min_interval(provider_key: str) -> None:
    """Sleep until at least provider_min_get_new_interval_seconds since the
    last get_new attempt for this provider — proactive spacing so we hit
    fewer vendor 405 cooldowns when hashtag/comments mint in parallel."""
    last = _last_get_new_at.get(provider_key)
    if last is None:
        return
    min_interval = float(get_setting("provider_min_get_new_interval_seconds"))
    remaining = min_interval - (time.monotonic() - last)
    if remaining <= 0:
        return
    logger.info(
        "proxiestrust_min_interval_wait",
        provider=provider_key,
        seconds=round(remaining, 1),
        min_interval_seconds=min_interval,
    )
    time.sleep(remaining)


def get_new_proxy(*, provider_key: str = "proxiestrust_default") -> NewProxy | None:
    """Requests a fresh IP on a proxiestrust.com plan. provider_key picks
    *which* proxy_providers row (plan/token/API URL/ip_allowlist mode) to
    use - this project has more than one proxiestrust account (e.g.
    proxiestrust_tiktok_us for TikTok's synthetic guest identity, see
    spiders/tiktok/client.py), each with its own exit country/rotation
    slot, so they must never be merged into a single provider.

    The provider's ip_allowlist=True passes the vendor's own ip_allow_on=on param and
    reads back proxy_ip_allow instead of proxy - confirmed live (2026-09-17)
    these draw from a *different*, apparently much less abused pool than
    the default username:password-authenticated `proxy` field, which kept
    cycling through the same ~4 already-TikTok-blocked IPs. proxiestrust
    auto-allowlists whatever IP this call itself came from (its own
    "hệ thống tự động lấy ip máy chạy tool của bạn"), so this only grants
    access to the machine that actually calls get_new - fine here since the
    same process calls get_new and then makes the proxied request itself,
    but this lease cannot be handed to a different machine/IP than the one
    that minted it.

    Blocks the calling thread (not just this call) for up to
    provider_max_cooldown_wait_seconds if the very first attempt hits the
    vendor's own rotation cooldown - see the module comment above for why
    waiting once is worth it here. Also enforces
    provider_min_get_new_interval_seconds between attempts for the same
    provider. Callers on an event loop should run this in a thread (e.g.
    asyncio.to_thread), same as any other blocking network call here.

    None when the provider has no token/API URL configured, the request
    (or its one cooldown retry) fails, or the response doesn't parse -
    every case already logged here so a caller just needs to treat None as
    "couldn't refresh, fall back to whatever it would have done anyway"."""
    provider = get_provider(provider_key)
    if provider is None or not provider["token"]:
        return None
    if not provider["api_url"]:
        logger.warning("proxy_provider_missing_api_url", provider=provider_key)
        return None
    token = provider["token"]
    ip_allowlist = provider["ip_allowlist"]
    request_timeout = float(get_setting("provider_request_timeout_seconds"))

    params = {"token": token}
    if ip_allowlist:
        params["ip_allow_on"] = "on"
    response_field = "proxy_ip_allow" if ip_allowlist else "proxy"
    parse = _parse_ip_allow_string if ip_allowlist else _parse_proxy_string

    for is_retry in (False, True):
        _wait_min_interval(provider_key)
        _last_get_new_at[provider_key] = time.monotonic()
        try:
            resp = requests.get(provider["api_url"], params=params, timeout=request_timeout)
            resp.raise_for_status()
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("proxiestrust_get_new_failed", provider=provider_key, error=_redact(str(exc), token))
            return None

        if body.get("status") == "SUCCESS":
            proxy = parse(str(body.get(response_field) or ""))
            if proxy is None:
                logger.warning("proxiestrust_get_new_unparseable", provider=provider_key, response=body)
                return None
            logger.info(
                "proxiestrust_get_new_ok",
                provider=provider_key,
                host=proxy["host"],
                ip_allowlist=ip_allowlist,
                time_seconds_to_die=body.get("time_seconds_to_die"),
                waited_out_cooldown=is_retry,
            )
            return proxy

        logger.warning("proxiestrust_get_new_rejected", provider=provider_key, response=body)
        if is_retry:
            return None  # already waited out one cooldown - a second rejection is a real failure

        wait_seconds = _cooldown_wait_seconds(str(body.get("error") or ""))
        if wait_seconds is None:
            return None  # rejected for some other reason - waiting wouldn't help
        logger.info("proxiestrust_waiting_out_cooldown", seconds=wait_seconds)
        time.sleep(wait_seconds)

    return None  # unreachable - satisfies type checkers


def _cooldown_wait_seconds(error_message: str) -> int | None:
    """Seconds to wait before get_new_proxy's one retry, parsed from the
    vendor's own "còn NN giây" rejection text - None if this doesn't look
    like a cooldown rejection at all (some other error), so the caller
    doesn't sleep for no reason."""
    match = _COOLDOWN_MESSAGE_PATTERN.search(error_message)
    if match is None:
        return None
    return min(int(match.group(1)) + 2, int(get_setting("provider_max_cooldown_wait_seconds")))
