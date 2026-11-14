import json
from datetime import datetime
from urllib.parse import unquote

from social_crawler.spiders.youtube.config import CLIENT_GL, CLIENT_HL, ENDPOINT_PLAYER, YOUTUBE_BASE_URL
from social_crawler.spiders.youtube.headers import get_youtube_headers
from social_crawler.spiders.youtube._compat import Logger
from social_crawler.spiders.youtube.client import create_httpx_client, get_context, get_youtube_api_key, get_youtube_api_url
from social_crawler.spiders.youtube.utils import parse_view_count

logger = Logger.get(__name__)


def _extract_player_response(html: str) -> dict | None:
    """
    Lấy ytInitialPlayerResponse từ HTML trang xem video của YouTube.
    Đếm ngoặc nhọn để tách khối JSON cho chắc.
    """
    idx = html.find("ytInitialPlayerResponse")
    if idx == -1:
        return None
    start = html.find("{", idx)
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(html)):
        c = html[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[start : i + 1])
                except json.JSONDecodeError:
                    return None
    return None


async def _get_via_watch_page(video_id: str, proxy: str = None) -> dict | None:
    headers = get_youtube_headers()
    params = {"v": video_id, "hl": CLIENT_HL, "gl": CLIENT_GL}
    async with create_httpx_client(proxy=proxy, headers=headers, timeout=15) as client:
        resp = await client.get(f"{YOUTUBE_BASE_URL}/watch", params=params)
    if resp.status_code != 200:
        logger.warning("[detail] watch_page HTTP %s cho %s", resp.status_code, video_id)
        return None
    data = _extract_player_response(resp.text)
    if not data:
        logger.warning("[detail] watch_page: không thấy ytInitialPlayerResponse của %s", video_id)
        return None
    return data


async def _get_via_api(video_id: str, proxy: str = None) -> dict | None:
    try:
        api_key = await get_youtube_api_key(proxy=proxy)
    except Exception as e:
        logger.warning("[detail] lấy API key lỗi: %s", e)
        return None
    url = get_youtube_api_url(ENDPOINT_PLAYER, api_key)
    headers = get_youtube_headers()
    payload = {
        "context": get_context(),
        "videoId": video_id,
        "contentCheckOk": True,
        "racyCheckOk": True,
    }
    async with create_httpx_client(proxy=proxy, headers=headers, timeout=10) as client:
        resp = await client.post(url, json=payload)
    if resp.status_code != 200:
        logger.warning("[detail] API HTTP %s cho %s", resp.status_code, video_id)
        return None
    return resp.json()


def _parse_player_response(data: dict, video_id: str) -> dict:
    status = data.get("playabilityStatus", {})
    yt_status = status.get("status")
    if yt_status != "OK":
        return {
            "error": True,
            "reason": status.get("reason", "Unavailable"),
            "status": yt_status,
        }

    video_details = data.get("videoDetails", {})
    streaming_data = data.get("streamingData", {})
    microformat = data.get("microformat", {}).get("playerMicroformatRenderer", {})
    description = video_details.get("shortDescription") or microformat.get("description", {}).get("simpleText") or ""
    thumbnails = video_details.get("thumbnail", {}).get("thumbnails", [])
    if not thumbnails:
        vid = video_details.get("videoId") or video_id
        thumbnails = [{"url": f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"}]

    return {
        "video_id": video_details.get("videoId") or video_id,
        "title": video_details.get("title"),
        "author": video_details.get("author"),
        "channel_id": video_details.get("channelId") or None,
        "description": description,
        "length_seconds": video_details.get("lengthSeconds"),
        "views": parse_view_count(video_details.get("viewCount")),
        "is_live_content": video_details.get("isLiveContent"),
        "thumbnails": thumbnails,
        "formats": streaming_data.get("formats", []),
        "adaptive_formats": streaming_data.get("adaptiveFormats", []),
        "keywords": video_details.get("keywords", []),
        "category": microformat.get("category", ""),
        "publish_date": microformat.get("publishDate", ""),
    }


async def get_video_detail(video_id: str, proxy: str = None) -> dict:
    data = await _get_via_watch_page(video_id, proxy=proxy)
    result = _parse_player_response(data, video_id) if data else {"error": True, "status": None}

    if result.get("error"):
        logger.info(
            "[detail] watch_page blocked, trying API%s, trying player API for %s",
            result.get("status"),
            video_id,
        )
        api_data = await _get_via_api(video_id, proxy=proxy)
        if api_data:
            result = _parse_player_response(api_data, video_id)

    if result.get("error"):
        logger.warning(
            "[detail] both methods failed for %s (status=%s)",
            video_id,
            result.get("status"),
        )

    return result


def _parse_meta(data: dict, video_id: str) -> dict | None:
    """Metadata cho kho bài: ngày đăng chính xác, view, like, mô tả đầy đủ. Khác _parse_player_response, video bị chặn phát
    (UNPLAYABLE - YouTube chặn luồng phát với client lạ) vẫn đọc được: videoDetails + microformat vẫn có đủ."""
    details = data.get("videoDetails") or {}
    if not details.get("videoId"):
        return None
    micro = data.get("microformat", {}).get("playerMicroformatRenderer", {})
    thumbs = details.get("thumbnail", {}).get("thumbnails") or []
    published = micro.get("publishDate") or micro.get("uploadDate")
    try:
        timestamp = int(datetime.fromisoformat(published).timestamp()) if published else None
    except ValueError:
        timestamp = None
    owner = unquote((micro.get("ownerProfileUrl") or "").rsplit("/", 1)[-1])
    return {
        "video_id": details.get("videoId") or video_id,
        "title": details.get("title"),
        "description": details.get("shortDescription") or micro.get("description", {}).get("simpleText") or "",
        "channel_name": details.get("author"),
        "channel_id": details.get("channelId"),
        "channel_handle": owner if owner.startswith("@") else None,
        "view_count": parse_view_count(details.get("viewCount")),
        "like_count": parse_view_count(micro.get("likeCount")),
        "duration_seconds": int(details.get("lengthSeconds") or 0),
        "timestamp": timestamp,
        "keywords": details.get("keywords", []),
        "category": micro.get("category", ""),
        "is_live": bool(details.get("isLiveContent")),
        "is_short": bool(micro.get("isShortsEligible")) and int(details.get("lengthSeconds") or 0) <= 180,
        "thumbnail": thumbs[-1]["url"] if thumbs else f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
    }


async def get_video_meta(video_id: str, proxy: str = None) -> dict | None:
    """API player trước (~16 KB), trang xem (~1,3 MB) chỉ khi API không trả videoDetails."""
    data = await _get_via_api(video_id, proxy=proxy)
    meta = _parse_meta(data, video_id) if data else None
    if meta is None:
        page = await _get_via_watch_page(video_id, proxy=proxy)
        meta = _parse_meta(page, video_id) if page else None
    return meta
