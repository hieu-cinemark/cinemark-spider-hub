import asyncio
import random
import re
import time

import httpx

from social_crawler.spiders.youtube.config import (
    CLIENT_GL,
    CLIENT_HL,
    CLIENT_NAME,
    CLIENT_VERSION,
    DEFAULT_TIMEOUT,
    HTTP_MAX_CONNECTIONS,
    HTTP_MAX_KEEPALIVE,
    INNERTUBE_API_KEY,
    YOUTUBE_API_BASE,
    YOUTUBE_BASE_URL,
    YOUTUBE_KEY_TTL,
)
from social_crawler.spiders.youtube._compat import Logger

logger = Logger.get(__name__)

_api_key_cache: dict = {"value": INNERTUBE_API_KEY, "expires": float("inf") if INNERTUBE_API_KEY else 0.0}
_key_lock = asyncio.Lock()
_client_pool: dict[str, httpx.AsyncClient] = {}
_visitor_data_cache: dict = {"value": "", "expires": 0.0}
_client_version_cache: dict = {"value": "", "expires": 0.0}


def _get_pooled_client(proxy: str | None, headers: dict | None, timeout: int) -> httpx.AsyncClient:
    key = proxy or ""
    if key not in _client_pool or _client_pool[key].is_closed:
        limits = httpx.Limits(
            max_connections=HTTP_MAX_CONNECTIONS,
            max_keepalive_connections=HTTP_MAX_KEEPALIVE,
        )
        kwargs: dict = {"timeout": timeout, "limits": limits}
        if proxy:
            kwargs["proxy"] = proxy
        if headers:
            kwargs["headers"] = headers
        _client_pool[key] = httpx.AsyncClient(**kwargs)
    return _client_pool[key]


async def get_youtube_api_key(proxy: str = None, force: bool = False) -> str:
    now = time.monotonic()
    if not force and _api_key_cache["value"] and now < _api_key_cache["expires"]:
        return _api_key_cache["value"]

    async with _key_lock:
        now = time.monotonic()
        if not force and _api_key_cache["value"] and now < _api_key_cache["expires"]:
            return _api_key_cache["value"]
        await _scrape_homepage(proxy)
        return _api_key_cache["value"]


def get_youtube_api_url(endpoint: str, api_key: str) -> str:
    return f"{YOUTUBE_API_BASE}/{endpoint}?key={api_key}" if api_key else f"{YOUTUBE_API_BASE}/{endpoint}?prettyPrint=false"


async def _scrape_homepage(proxy: str = None) -> None:
    now = time.monotonic()
    try:
        async with create_httpx_client(proxy=proxy) as client:
            resp = await client.get(YOUTUBE_BASE_URL)
            html = resp.text

        match = re.search(r'"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"', html)
        if match:
            _api_key_cache["value"] = match.group(1)
            _api_key_cache["expires"] = now + YOUTUBE_KEY_TTL

        vd_match = re.search(r'"visitorData"\s*:\s*"([^"]+)"', html)
        if vd_match:
            _visitor_data_cache["value"] = vd_match.group(1)
            _visitor_data_cache["expires"] = now + YOUTUBE_KEY_TTL

        cv_match = re.search(r'"INNERTUBE_CLIENT_VERSION"\s*:\s*"([^"]+)"', html)
        if cv_match:
            _client_version_cache["value"] = cv_match.group(1)
            _client_version_cache["expires"] = now + YOUTUBE_KEY_TTL
    except Exception as e:
        logger.warning(" Homepage scrape failed (using fallback key): %s", e)


async def warm_youtube_session(proxy: str = None) -> None:
    """Fire-and-forget: warm visitorData/client_version without blocking searches."""
    if get_visitor_data():
        return
    async with _key_lock:
        if get_visitor_data():
            return
        await _scrape_homepage(proxy)


def get_visitor_data() -> str | None:
    """Return cached visitorData if fresh, else None."""
    if _visitor_data_cache["value"] and time.monotonic() < _visitor_data_cache["expires"]:
        return _visitor_data_cache["value"]
    return None


def get_client_version() -> str:
    """Return dynamic client version from homepage, fallback to constant."""
    if _client_version_cache["value"] and time.monotonic() < _client_version_cache["expires"]:
        return _client_version_cache["value"]
    return CLIENT_VERSION


_TIMEZONES = [
    "America/New_York",
    "America/Chicago",
    "America/Los_Angeles",
    "Europe/London",
    "Europe/Paris",
    "Asia/Tokyo",
    "Asia/Ho_Chi_Minh",
    "Asia/Bangkok",
    "Asia/Singapore",
    "Australia/Sydney",
]


def get_context(original_url: str | None = None, user_agent: str | None = None) -> dict:
    """Full InnerTube WEB context. browse endpoints require user + request sections."""
    client: dict = {
        "hl": CLIENT_HL,
        "gl": CLIENT_GL,
        "clientName": CLIENT_NAME,
        "clientVersion": get_client_version(),
        "platform": "DESKTOP",
        "clientFormFactor": "UNKNOWN_FORM_FACTOR",
        "timeZone": random.choice(_TIMEZONES),
        "utcOffsetMinutes": random.choice([-300, -360, -420, 0, 60, 120, 420, 540, 600]),
        "screenWidthPoints": random.choice([1280, 1366, 1440, 1536, 1920, 2560]),
        "screenHeightPoints": random.choice([720, 768, 864, 900, 1080, 1440]),
        "screenPixelDensity": random.choice([1, 2]),
    }

    visitor_data = get_visitor_data()
    if visitor_data:
        client["visitorData"] = visitor_data
    if original_url:
        client["originalUrl"] = original_url
    if user_agent:
        client["userAgent"] = user_agent

    return {
        "client": client,
        "user": {"lockedSafetyMode": False},
        "request": {
            "useSsl": True,
            "internalExperimentFlags": [],
            "consistencyTokenJars": [],
        },
    }


async def resolve_channel_id_from_handle(handle: str) -> str:
    async with create_httpx_client() as client:
        url = f"{YOUTUBE_BASE_URL}/@{handle}"
        resp = await client.get(url)
        html = resp.text
        match = re.search(r"channel_id=([a-zA-Z0-9_-]{24})", html)
        if match:
            return match.group(1)
        match = re.search(r'"browseId":"(UC[^\"]+)"', html)
        if match:
            return match.group(1)
        raise Exception("Channel_id not found")


def get_httpx_proxies(proxy: str = None):
    return proxy or None


class _PooledClientContext:
    """Async context manager that yields a shared client without closing it on exit."""

    __slots__ = ("_client",)

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def __aenter__(self) -> httpx.AsyncClient:
        return self._client

    async def __aexit__(self, *_) -> None:
        pass


def create_httpx_client(proxy: str = None, headers: dict = None, timeout: int = DEFAULT_TIMEOUT):
    proxy_url = get_httpx_proxies(proxy)
    if not headers:
        return _PooledClientContext(_get_pooled_client(proxy_url, None, timeout))

    kwargs: dict = {"timeout": timeout}
    kwargs["headers"] = headers
    if proxy_url:
        kwargs["proxy"] = proxy_url
    return httpx.AsyncClient(**kwargs)
