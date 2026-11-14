import base64

from social_crawler.spiders.youtube.config import CHANNEL_TAB_VIDEOS, ENDPOINT_BROWSE, YOUTUBE_BASE_URL
from social_crawler.spiders.youtube.headers import get_youtube_headers
from social_crawler.spiders.youtube.client import create_httpx_client, get_context, get_youtube_api_key, get_youtube_api_url

from social_crawler.spiders.youtube._compat import YouTubeStructureChangedError
from ..shared.parsers import find_tab_by_url_suffix, join_runs


def _extract_lockup_video(lockup: dict) -> dict | None:
    """Đọc một video trong lưới kênh ở dạng `lockupViewModel` mới
    (YouTube đang chuyển lưới kênh/playlist sang mô hình component này;
    nó thay cho dạng cũ `videoRenderer`/`gridVideoRenderer`)."""
    if lockup.get("contentType") != "LOCKUP_CONTENT_TYPE_VIDEO":
        return None
    video_id = lockup.get("contentId")
    metadata_vm = lockup.get("metadata", {}).get("lockupMetadataViewModel", {})
    title = metadata_vm.get("title", {}).get("content", "")
    rows = metadata_vm.get("metadata", {}).get("contentMetadataViewModel", {}).get("metadataRows", [])
    parts = rows[0].get("metadataParts", []) if rows else []
    views = parts[0].get("text", {}).get("content", "") if len(parts) > 0 else ""
    published = parts[1].get("text", {}).get("content", "") if len(parts) > 1 else ""
    thumbnail_vm = lockup.get("contentImage", {}).get("thumbnailViewModel", {})
    duration = ""
    for overlay in thumbnail_vm.get("overlays", []):
        badges = overlay.get("thumbnailBottomOverlayViewModel", {}).get("badges", [])
        if badges:
            duration = badges[0].get("thumbnailBadgeViewModel", {}).get("text", "")
            break
    return {
        "title": title,
        "videoId": video_id,
        "url": f"{YOUTUBE_BASE_URL}/watch?v={video_id}",
        "duration": duration,
        "views": views,
        "thumbnail": thumbnail_vm.get("image", {}).get("sources", []),
        "public": published,
    }


def extract_video_items(items: list[dict]) -> list[dict]:
    videos = []
    for item in items:
        content = item.get("richItemRenderer", {}).get("content", {}) or item
        lockup = content.get("lockupViewModel")
        if lockup:
            parsed = _extract_lockup_video(lockup)
            if parsed:
                videos.append(parsed)
            continue
        video = content.get("videoRenderer") or content.get("gridVideoRenderer")
        if not video:
            continue
        videos.append(
            {
                "title": join_runs(video.get("title", {})),
                "videoId": video.get("videoId"),
                "url": f"{YOUTUBE_BASE_URL}/watch?v={video.get('videoId')}",
                "duration": video.get("lengthText", {}).get("simpleText", ""),
                "views": video.get("viewCountText", {}).get("simpleText", ""),
                "thumbnail": video.get("thumbnail", {}).get("thumbnails", []),
                "public": video.get("publishedTimeText", {}).get("simpleText", ""),
            }
        )
    return videos


async def get_channel_videos(channel_id: str, proxy: str = None, max_results: int = 100) -> list[dict]:
    api_key = await get_youtube_api_key(proxy=proxy)
    browse_url = get_youtube_api_url(ENDPOINT_BROWSE, api_key)
    headers = get_youtube_headers()
    collected = []
    continuation = None

    encoded = CHANNEL_TAB_VIDEOS
    missing_padding = len(encoded) % 4
    if missing_padding:
        encoded += "=" * (4 - missing_padding)
    decoded = base64.b64decode(encoded).decode("utf-8")

    async with create_httpx_client(proxy=proxy, headers=headers) as client:
        payload = {"context": get_context(), "browseId": channel_id, "params": decoded}
        resp = await client.post(browse_url, json=payload)
        resp.raise_for_status()
        data = resp.json()

        tabs = data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
        if not tabs:
            raise YouTubeStructureChangedError(
                "twoColumnBrowseResultsRenderer.tabs not found for channel",
                context={"channel_id": channel_id},
            )

        videos_tab = find_tab_by_url_suffix(tabs, "/videos")
        if videos_tab:
            endpoint = videos_tab.get("endpoint", {}).get("browseEndpoint", {})
            browse_id = endpoint.get("browseId")
            params = endpoint.get("params")
            payload = {
                "context": get_context(),
                "browseId": browse_id,
                "params": params,
            }
            resp = await client.post(browse_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            tabs = data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
            videos_tab = find_tab_by_url_suffix(tabs, "/videos")

        if not videos_tab:
            videos_tab = find_tab_by_url_suffix(tabs, "/featured")
            if not videos_tab:
                raise YouTubeStructureChangedError(
                    "Neither 'Videos' nor 'Home' tab found for channel",
                    context={"channel_id": channel_id},
                )

        section = videos_tab.get("content", {}).get("richGridRenderer", {}).get("contents", [])
        collected += extract_video_items(section)
        continuation = next(
            (
                c.get("continuationItemRenderer", {})
                .get("continuationEndpoint", {})
                .get("continuationCommand", {})
                .get("token")
                for c in section
                if "continuationItemRenderer" in c
            ),
            None,
        )

        while continuation and len(collected) < max_results:
            payload = {"context": get_context(), "continuation": continuation}
            resp = await client.post(browse_url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            commands = data.get("onResponseReceivedCommands") or data.get("onResponseReceivedActions") or []
            if not commands:
                break
            continuation_items = commands[0].get("appendContinuationItemsAction", {}).get("continuationItems", [])
            collected += extract_video_items(continuation_items)
            continuation = next(
                (
                    i.get("continuationItemRenderer", {})
                    .get("continuationEndpoint", {})
                    .get("continuationCommand", {})
                    .get("token")
                    for i in continuation_items
                    if "continuationItemRenderer" in i
                ),
                None,
            )

    return collected[:max_results]
