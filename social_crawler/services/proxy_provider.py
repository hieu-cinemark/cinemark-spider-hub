"""Thin client for proxiestrust.com's "get new proxy" API - lets
services/pool.py request a fresh IP on an already-purchased rotating slot
once the pool's own circuit breaker (see pool.REPIN_AFTER_CONSECUTIVE_
FAILURES) decides a proxiestrust-sourced platform_proxies row is dead,
instead of just abandoning that row and re-pinning accounts elsewhere
forever (which only ever shrinks the usable pool - nothing else in this
project ever revives a dead proxy row).

Degrades to None (not an exception) when the relevant token env var isn't
set, same convention as services/kira.py - a proxiestrust proxy going
unrefreshed should behave exactly like it did before this module existed
(abandoned, account re-pinned elsewhere), not crash whatever called into
this.

This project has more than one proxiestrust.com plan/token (different exit
countries, purchased for different platforms) - get_new_proxy()'s
token_env_var picks which one; never default multiple call sites onto the
same env var just because they both happen to call this module."""

from __future__ import annotations

import os
import re
import time
from typing import TypedDict

import requests

from social_crawler import env  # noqa: F401 - import for its load_dotenv() side effect
from social_crawler.logger import get_logger

logger = get_logger(__name__)

_GET_NEW_URL = "https://proxiestrust.com/sp07api/get_new"
_REQUEST_TIMEOUT_SECONDS = 10

# The vendor's own per-token cooldown between two get_new calls (observed
# 2026-09-17: ~90s; operator-confirmed minimum spacing ~60s) rejects an
# early call with statusCode 405 and a Vietnamese "còn NN giây" message
# rather than handing back the still-live previous lease. Callers that
# retry immediately would fall back to a worse proxy source instead of
# getting a fresh IP. We therefore:
#   1. Space our own calls at least _MIN_GET_NEW_INTERVAL_SECONDS apart
#      (per token) so we usually never hit the 405 in the first place.
#   2. If we still get a cooldown rejection, wait once (capped) and retry.
_MIN_GET_NEW_INTERVAL_SECONDS = 60.0
_MAX_COOLDOWN_WAIT_SECONDS = 120
_COOLDOWN_MESSAGE_PATTERN = re.compile(r"(\d+)")

# Monotonic timestamp of the last get_new HTTP attempt per token env var
# name — enforces _MIN_GET_NEW_INTERVAL_SECONDS across hashtag + comments
# synthetic mints sharing PROXIESTRUST_TIKTOK_US_API_TOKEN.
_last_get_new_at: dict[str, float] = {}


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


def _wait_min_interval(token_env_var: str) -> None:
    """Sleep until at least _MIN_GET_NEW_INTERVAL_SECONDS since the last
    get_new attempt for this token — proactive spacing so we hit fewer
    vendor 405 cooldowns when hashtag/comments mint in parallel."""
    last = _last_get_new_at.get(token_env_var)
    if last is None:
        return
    remaining = _MIN_GET_NEW_INTERVAL_SECONDS - (time.monotonic() - last)
    if remaining <= 0:
        return
    logger.info(
        "proxiestrust_min_interval_wait",
        token_env_var=token_env_var,
        seconds=round(remaining, 1),
        min_interval_seconds=_MIN_GET_NEW_INTERVAL_SECONDS,
    )
    time.sleep(remaining)


def get_new_proxy(
    *, token_env_var: str = "PROXIESTRUST_API_TOKEN", ip_allowlist: bool = False
) -> NewProxy | None:
    """Requests a fresh IP on a proxiestrust.com plan. token_env_var picks
    *which* plan/token to use - this project has more than one proxiestrust
    account (e.g. PROXIESTRUST_TIKTOK_US_API_TOKEN for TikTok's synthetic
    guest identity, see spiders/tiktok/client.py), each with its own exit
    country/rotation slot, so they must never be merged into a single env
    var. Defaults to PROXIESTRUST_API_TOKEN (this module's original,
    pool.py-facing plan) for backward compatibility.

    ip_allowlist=True passes the vendor's own ip_allow_on=on param and
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
    _MAX_COOLDOWN_WAIT_SECONDS if the very first attempt hits the vendor's
    own rotation cooldown - see that constant's own comment for why waiting
    once is worth it here. Also enforces _MIN_GET_NEW_INTERVAL_SECONDS
    between attempts for the same token. Callers on an event loop should
    run this in a thread (e.g. asyncio.to_thread), same as any other
    blocking network call in this project.

    None when that env var isn't configured, the request (or its one
    cooldown retry) fails, or the response doesn't parse - every case
    already logged here so a caller just needs to treat None as "couldn't
    refresh, fall back to whatever it would have done anyway"."""
    token = os.getenv(token_env_var)
    if not token:
        return None

    params = {"token": token}
    if ip_allowlist:
        params["ip_allow_on"] = "on"
    response_field = "proxy_ip_allow" if ip_allowlist else "proxy"
    parse = _parse_ip_allow_string if ip_allowlist else _parse_proxy_string

    for is_retry in (False, True):
        _wait_min_interval(token_env_var)
        _last_get_new_at[token_env_var] = time.monotonic()
        try:
            resp = requests.get(_GET_NEW_URL, params=params, timeout=_REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            body = resp.json()
        except (requests.RequestException, ValueError) as exc:
            logger.warning("proxiestrust_get_new_failed", error=str(exc))
            return None

        if body.get("status") == "SUCCESS":
            proxy = parse(str(body.get(response_field) or ""))
            if proxy is None:
                logger.warning("proxiestrust_get_new_unparseable", response=body)
                return None
            logger.info(
                "proxiestrust_get_new_ok",
                host=proxy["host"],
                ip_allowlist=ip_allowlist,
                time_seconds_to_die=body.get("time_seconds_to_die"),
                waited_out_cooldown=is_retry,
            )
            return proxy

        logger.warning("proxiestrust_get_new_rejected", response=body)
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
    return min(int(match.group(1)) + 2, _MAX_COOLDOWN_WAIT_SECONDS)
