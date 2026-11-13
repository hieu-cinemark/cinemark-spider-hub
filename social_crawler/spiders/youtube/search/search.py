from urllib.parse import unquote

from social_crawler.spiders.youtube.config import ENDPOINT_SEARCH
from social_crawler.spiders.youtube.headers import get_youtube_headers
from social_crawler.spiders.youtube.utils import parse_view_count
from social_crawler.spiders.youtube.client import create_httpx_client, get_context, get_youtube_api_key, get_youtube_api_url

from social_crawler.spiders.youtube._compat import YouTubeStructureChangedError
from ..shared.parsers import extract_continuation_token, get_continuation_items, parse_video_renderer
from .search_constants import SORT_OPTIONS


def extract_video_items(items: list[dict]) -> list[dict]:
    results = []
    for item in items:
        if "richItemRenderer" in item:
            video = item["richItemRenderer"].get("content", {}).get("videoRenderer")
        else:
            video = item.get("videoRenderer")
        if not video or not video.get("videoId"):
            continue
        parsed = parse_video_renderer(video)
        runs = video.get("detailedMetadataSnippets", [{}])[0].get("snippetText", {}).get("runs", [])
        parsed["description_snippet"] = "".join(r.get("text", "") for r in runs)
        results.append(parsed)
    return results


async def search_youtube(query: str, max_results: int = 20, proxy: str = None, sort: str = "relevance") -> list[dict]:
    api_key = await get_youtube_api_key(proxy=proxy)
    search_url = get_youtube_api_url(ENDPOINT_SEARCH, api_key)
    headers = get_youtube_headers()
    sort_param = SORT_OPTIONS.get(sort)

    collected = []
    continuation = None

    async with create_httpx_client(proxy=proxy, headers=headers) as client:
        payload = {"context": get_context(), "query": query}
        if sort_param:
            payload["params"] = sort_param

        resp = await client.post(search_url, json=payload)
        resp.raise_for_status()
        data = resp.json()

        sections = (
            data.get("contents", {})
            .get("twoColumnSearchResultsRenderer", {})
            .get("primaryContents", {})
            .get("sectionListRenderer", {})
            .get("contents")
        )
        if not sections:
            raise YouTubeStructureChangedError(
                "sectionListRenderer.contents not found in search response",
                context={"top_keys": list(data.get("contents", {}).keys())},
            )

        for section in sections:
            if "itemSectionRenderer" in section:
                collected += extract_video_items(section["itemSectionRenderer"].get("contents", []))
            if "continuationItemRenderer" in section:
                continuation = extract_continuation_token(section)

        ctx = get_context()
        while continuation and len(collected) < max_results:
            token, continuation = continuation, None
            payload = {"context": ctx, "continuation": token}
            resp = await client.post(search_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            for section in get_continuation_items(data):
                if "itemSectionRenderer" in section:
                    collected += extract_video_items(section["itemSectionRenderer"].get("contents", []))
                if "continuationItemRenderer" in section:
                    continuation = extract_continuation_token(section)

    deduped = list({v["video_id"]: v for v in collected if v.get("video_id")}.values())
    return deduped[:max_results]


def _walk(node, key: str):
    if isinstance(node, dict):
        for k, value in node.items():
            if k == key:
                yield value
            yield from _walk(value, key)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value, key)


def extract_shorts(data: dict) -> list[dict]:
    """Shorts trong trang tìm kiếm: kệ gridShelfViewModel/reelShelfRenderer chứa shortsLockupViewModel. extract_video_items
    chỉ đọc videoRenderer nên bỏ sót toàn bộ Shorts."""
    out = []
    for lockup in _walk(data, "shortsLockupViewModel"):
        if not isinstance(lockup, dict):
            continue
        endpoint = lockup.get("onTap", {}).get("innertubeCommand", {}).get("reelWatchEndpoint", {})
        video_id = endpoint.get("videoId")
        if not video_id:
            continue
        overlay = lockup.get("overlayMetadata", {})
        thumbs = endpoint.get("thumbnail", {}).get("thumbnails") or []
        title = overlay.get("primaryText", {}).get("content") or lockup.get("accessibilityText", "").split(",")[0]
        out.append(
            {
                "video_id": video_id,
                "title": title,
                "channel": None,
                "channel_id": None,
                "view_count": parse_view_count(overlay.get("secondaryText", {}).get("content")),
                "duration": "",
                "published_time": "",
                "thumbnails": thumbs,
                "url": f"https://www.youtube.com/shorts/{video_id}",
                "description_snippet": "",
                "is_short": True,
            }
        )
    return out


def _page_videos(data: dict) -> list[dict]:
    videos = []
    for section in _walk(data, "itemSectionRenderer"):
        videos += extract_video_items(section.get("contents", []))
    for video in videos:
        video.setdefault("is_short", "/shorts/" in str(video.get("url")))
    seen: set[str] = set()
    return [v for v in videos + extract_shorts(data) if not (v["video_id"] in seen or seen.add(v["video_id"]))]


def _next_token(data: dict) -> str | None:
    """Token trang kế của danh sách kết quả chính - continuationItemRenderer nằm thẳng trong sectionListRenderer.contents
    (trang đầu) hoặc continuationItems (trang sau). Không lấy continuationCommand bất kỳ: kệ con (Shorts, "Mọi người cũng
    xem") có token riêng, lấy nhầm là lật lại cùng một kệ."""
    sections = (
        data.get("contents", {})
        .get("twoColumnSearchResultsRenderer", {})
        .get("primaryContents", {})
        .get("sectionListRenderer", {})
        .get("contents")
    ) or get_continuation_items(data)
    for section in sections or []:
        if "continuationItemRenderer" in section:
            return extract_continuation_token(section)
    return None


async def search_pages(query: str, *, filter_param: str | None = None, max_pages: int = 20, proxy: str = None):
    """Lật từng trang kết quả tìm kiếm (video thường + Shorts), yield (số trang, danh sách video) để chỗ gọi dừng sớm khi
    toàn bài đã thấy. max_pages=0 là không giới hạn (tới khi YouTube hết trang)."""
    api_key = await get_youtube_api_key(proxy=proxy)
    search_url = get_youtube_api_url(ENDPOINT_SEARCH, api_key)
    async with create_httpx_client(proxy=proxy, headers=get_youtube_headers()) as client:
        payload = {"context": get_context(), "query": query}
        if filter_param:
            # InnerTube nhận base64 trần; dạng mã hoá URL ("...%3D%3D") bị bỏ qua lặng lẽ (trả như không lọc).
            payload["params"] = unquote(filter_param)
        page = 0
        while True:
            resp = await client.post(search_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            page += 1
            yield page, _page_videos(data)
            token = _next_token(data)
            if not token or (max_pages and page >= max_pages):
                return
            payload = {"context": get_context(), "continuation": token}
