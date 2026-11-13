from social_crawler.spiders.youtube.config import ENDPOINT_BROWSE
from social_crawler.spiders.youtube.client import create_httpx_client, get_context, get_youtube_api_key, get_youtube_api_url

from social_crawler.spiders.youtube._compat import YouTubeStructureChangedError
from ..shared.parsers import find_tab_by_url_suffix


def extract_playlists_tab_info(data):
    tabs = data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
    videos_tab = find_tab_by_url_suffix(tabs, "/videos")
    endpoint = (videos_tab or {}).get("endpoint", {}).get("browseEndpoint", {})
    browse_id = endpoint.get("browseId")
    params = endpoint.get("params")
    if not browse_id or not params:
        raise YouTubeStructureChangedError(
            "Cannot find browseId or params for the Videos tab",
            context={"tabs_count": len(tabs)},
        )
    return browse_id, params


def extract_title(title_obj):
    if "simpleText" in title_obj:
        return title_obj["simpleText"]
    elif "runs" in title_obj and title_obj["runs"]:
        return "".join([run.get("text", "") for run in title_obj["runs"]])
    return ""


async def get_playlist_videos(channel_id: str, proxy: str = None) -> list[dict]:
    api_key = await get_youtube_api_key(proxy=proxy)
    browse_url = get_youtube_api_url(ENDPOINT_BROWSE, api_key)
    playlists = []

    async with create_httpx_client(proxy=proxy) as client:
        resp = await client.post(browse_url, json={"context": get_context(), "browseId": channel_id})
        resp.raise_for_status()
        data = resp.json()

        browse_id, params = extract_playlists_tab_info(data)
        if not browse_id or not params:
            raise YouTubeStructureChangedError(
                "browseId and params not found after first browse request",
                context={"channel_id": channel_id},
            )

        resp = await client.post(
            browse_url,
            json={"context": get_context(), "browseId": browse_id, "params": params},
        )
        resp.raise_for_status()
        playlist_data = resp.json()

        contents = playlist_data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
        target_tab = find_tab_by_url_suffix(contents, "/playlists")

        if not target_tab or "content" not in target_tab:
            endpoint = (target_tab or {}).get("endpoint", {}).get("browseEndpoint", {})
            browse_id = endpoint.get("browseId")
            params = endpoint.get("params")
            if not browse_id or not params:
                raise YouTubeStructureChangedError(
                    "browseId and params not found for Playlists tab endpoint",
                    context={"channel_id": channel_id},
                )
            resp = await client.post(
                browse_url,
                json={
                    "context": get_context(),
                    "browseId": browse_id,
                    "params": params,
                },
            )
            resp.raise_for_status()
            playlist_data = resp.json()
            contents = playlist_data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
            target_tab = find_tab_by_url_suffix(contents, "/playlists")
            if not target_tab:
                raise YouTubeStructureChangedError(
                    "Playlists tab not found after second browseEndpoint request",
                    context={"channel_id": channel_id},
                )

        contents = target_tab.get("content", {}).get("sectionListRenderer", {})
        for section in contents.get("contents", []):
            for item in section.get("itemSectionRenderer", {}).get("contents", []):
                for grid_item in item.get("gridRenderer", {}).get("items", []):
                    lockup = grid_item.get("lockupViewModel", {})
                    thumbnail_url = (
                        lockup.get("contentImage", {})
                        .get("collectionThumbnailViewModel", {})
                        .get("primaryThumbnail", {})
                        .get("thumbnailViewModel", {})
                        .get("image", {})
                        .get("sources", [{}])[-1]
                        .get("url", "")
                    )
                    video_count = ""
                    for overlay in (
                        lockup.get("contentImage", {})
                        .get("collectionThumbnailViewModel", {})
                        .get("primaryThumbnail", {})
                        .get("thumbnailViewModel", {})
                        .get("overlays", [])
                    ):
                        badge = overlay.get("thumbnailOverlayBadgeViewModel", {}).get("thumbnailBadges", [])
                        if badge:
                            video_count = badge[0].get("thumbnailBadgeViewModel", {}).get("text", "")
                            if video_count:
                                break
                    title = (
                        lockup.get("metadata", {})
                        .get("lockupMetadataViewModel", {})
                        .get("title", {})
                        .get("content", "")
                    )
                    playlist_id = lockup.get("contentId", "")
                    playlists.append(
                        {
                            "playlistId": playlist_id,
                            "title": title,
                            "thumbnail": thumbnail_url,
                            "videoCount": video_count,
                        }
                    )

    return playlists


async def get_videos_from_playlist(playlist_id: str, proxy: str = None) -> list[dict]:
    api_key = await get_youtube_api_key(proxy=proxy)
    browse_url = get_youtube_api_url(ENDPOINT_BROWSE, api_key)
    payload = {"context": get_context(), "browseId": f"VL{playlist_id}"}
    videos = []

    async with create_httpx_client(proxy=proxy) as client:
        while True:
            resp = await client.post(browse_url, json=payload)
            resp.raise_for_status()
            data = resp.json()

            if "contents" not in data:
                raise YouTubeStructureChangedError(
                    "Top-level 'contents' missing in playlist response",
                    context={"playlist_id": playlist_id, "top_keys": list(data.keys())},
                )

            tabs = data.get("contents", {}).get("twoColumnBrowseResultsRenderer", {}).get("tabs", [])
            contents = (
                (tabs[0] if tabs else {})
                .get("tabRenderer", {})
                .get("content", {})
                .get(
                    "sectionListRenderer",
                )
                .get("contents", [])
            )
            playlist_items_list = (contents[0] if contents else {}).get("itemSectionRenderer", {}).get("contents", [])
            contents = (
                (playlist_items_list[0] if playlist_items_list else {})
                .get("playlistVideoListRenderer", {})
                .get("contents", [])
            )
            if not contents:
                break

            continuation_token = None
            for item in contents:
                if "playlistVideoRenderer" in item:
                    renderer = item["playlistVideoRenderer"]
                    videos.append(
                        {
                            "video_id": renderer.get("videoId"),
                            "title": extract_title(renderer.get("title", {})),
                            "published_time": renderer.get("publishedTimeText", {}).get("simpleText", ""),
                            "duration": renderer.get("lengthText", {}).get("simpleText", ""),
                            "thumbnail": renderer.get("thumbnail", {}).get("thumbnails", [{}])[-1].get("url", ""),
                        }
                    )
                elif "continuationItemRenderer" in item:
                    continuation_token = (
                        item.get("continuationItemRenderer", {})
                        .get("continuationEndpoint", {})
                        .get("continuationCommand", {})
                        .get("token")
                    )

            if continuation_token:
                payload = {"context": get_context(), "continuation": continuation_token}
            else:
                break

    return videos
