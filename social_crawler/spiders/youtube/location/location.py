import random

from social_crawler.spiders.youtube.config import CLIENT_NAME, ENDPOINT_SEARCH
from social_crawler.spiders.youtube.headers import get_youtube_headers
from social_crawler.spiders.youtube._compat import Logger
from social_crawler.spiders.youtube.client import (
    create_httpx_client,
    get_client_version,
    get_visitor_data,
    get_youtube_api_key,
    get_youtube_api_url,
)

from social_crawler.spiders.youtube._compat import YouTubeStructureChangedError
from ..shared.parsers import extract_continuation_token, get_continuation_items, parse_video_renderer
from .location_constants import _GL_TIMEZONE

logger = Logger.get(__name__)


def _get_region_context(gl: str, hl: str) -> dict:
    """InnerTube context with overridden gl/hl for geographic targeting."""
    client: dict = {
        "hl": hl,
        "gl": gl,
        "clientName": CLIENT_NAME,
        "clientVersion": get_client_version(),
        "platform": "DESKTOP",
        "clientFormFactor": "UNKNOWN_FORM_FACTOR",
        "timeZone": _GL_TIMEZONE.get(gl, "America/New_York"),
        "screenWidthPoints": random.choice([1280, 1366, 1440, 1920]),
        "screenHeightPoints": random.choice([720, 768, 900, 1080]),
        "screenPixelDensity": random.choice([1, 2]),
    }
    visitor_data = get_visitor_data()
    if visitor_data:
        client["visitorData"] = visitor_data
    return {
        "client": client,
        "user": {"lockedSafetyMode": False},
        "request": {
            "useSsl": True,
            "internalExperimentFlags": [],
            "consistencyTokenJars": [],
        },
    }


def _extract_videos(items: list[dict]) -> list[dict]:
    results = []
    for item in items:
        if "richItemRenderer" in item:
            video = item["richItemRenderer"].get("content", {}).get("videoRenderer")
        else:
            video = item.get("videoRenderer")
        if not video or not video.get("videoId"):
            continue
        results.append(parse_video_renderer(video))
    return results


async def get_videos_by_region(
    gl: str,
    hl: str,
    query: str,
    proxy: str | None = None,
    max_results: int = 50,
) -> list[dict]:
    """Search YouTube with a specific country context (gl/hl) to get region-relevant results."""
    api_key = await get_youtube_api_key(proxy=proxy)
    search_url = get_youtube_api_url(ENDPOINT_SEARCH, api_key)
    headers = get_youtube_headers()
    context = _get_region_context(gl, hl)
    collected: list[dict] = []

    async with create_httpx_client(proxy=proxy, headers=headers) as client:
        resp = await client.post(search_url, json={"context": context, "query": query})
        resp.raise_for_status()
        data = resp.json()

        section_contents = (
            data.get("contents", {})
            .get("twoColumnSearchResultsRenderer", {})
            .get("primaryContents", {})
            .get("sectionListRenderer", {})
            .get("contents", [])
        )
        if not section_contents:
            raise YouTubeStructureChangedError(
                "sectionListRenderer.contents not found in region search response",
                context={
                    "gl": gl,
                    "query": query,
                    "top_keys": list(data.get("contents", {}).keys()),
                },
            )

        continuation = None
        for section in section_contents:
            if "itemSectionRenderer" in section:
                collected.extend(_extract_videos(section["itemSectionRenderer"].get("contents", [])))
            if "continuationItemRenderer" in section:
                continuation = extract_continuation_token(section)

        while continuation and len(collected) < max_results:
            resp = await client.post(search_url, json={"context": context, "continuation": continuation})
            resp.raise_for_status()
            data = resp.json()
            continuation = None
            for section in get_continuation_items(data):
                if "itemSectionRenderer" in section:
                    collected.extend(_extract_videos(section["itemSectionRenderer"].get("contents", [])))
                if "continuationItemRenderer" in section:
                    continuation = extract_continuation_token(section)

    seen: set = set()
    unique: list[dict] = []
    for v in collected:
        vid = v.get("video_id")
        if vid and vid not in seen:
            seen.add(vid)
            unique.append(v)

    logger.info("[location] gl=%s query='%s' -> %s video", gl, query, len(unique))
    return unique[:max_results]
