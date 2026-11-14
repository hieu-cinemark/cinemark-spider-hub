import asyncio
import base64
from typing import Literal

from social_crawler.spiders.youtube.config import ENDPOINT_NEXT
from social_crawler.spiders.youtube._compat import Logger
from social_crawler.spiders.youtube.client import create_httpx_client, get_context, get_youtube_api_key, get_youtube_api_url
from social_crawler.spiders.youtube.utils import parse_view_count

logger = Logger.get(__name__)


def _build_comment_sort_token(video_id: str, sort: str = "top") -> str:
    vid = video_id.encode()
    n = len(vid)

    inner_vid = bytes([0x12, n]) + vid
    f2 = bytes([0x12, len(inner_vid)]) + inner_vid
    f3 = bytes([0x18, 0x06]) if sort == "newest" else b""

    inner4 = bytes([0x22, n]) + vid + bytes([0x30, 0x01, 0x78, 0x02])
    f4 = bytes([0x22, len(inner4)]) + inner4
    f8 = bytes([0x42, 0x10]) + b"comments-section"
    f6_content = f4 + f8
    f6 = bytes([0x32, len(f6_content)]) + f6_content

    return base64.urlsafe_b64encode(f2 + f3 + f6).decode().rstrip("=")


async def fetch_replies(
    client,
    continuation_token: str,
    context: dict,
    proxy: str = None,
    max_depth: int = 2,
) -> list[dict]:
    replies = []
    api_key = await get_youtube_api_key(proxy=proxy)
    url_comment = get_youtube_api_url(ENDPOINT_NEXT, api_key)
    depth = 0

    while continuation_token and depth < max_depth:
        payload = {"context": context, "continuation": continuation_token}
        resp = await client.post(url_comment, json=payload)
        resp.raise_for_status()
        data = resp.json()
        entity_map = parse_comment_entities(data)
        continuation_token = None
        depth += 1

        for action in data.get("onResponseReceivedEndpoints", []):
            for item in action.get("appendContinuationItemsAction", {}).get("continuationItems", []):
                if "commentViewModel" in item:
                    comment_vm = item.get("commentViewModel", {})
                    comment_id = comment_vm.get("commentId")
                    entity = entity_map.get(comment_id, {})
                    if not entity:
                        logger.debug("Thiếu entity của phản hồi commentId=%s", comment_id)
                        continue
                    replies.append(
                        {
                            "comment_id": comment_id,
                            "author": entity.get("author", ""),
                            "avatar": entity.get("avatar"),
                            "content": entity.get("content", ""),
                            "published_time": entity.get("published_time", ""),
                            "likes": entity.get("likes", 0),
                        }
                    )
                elif "continuationItemRenderer" in item:
                    continuation_token = (
                        item.get("continuationItemRenderer", {})
                        .get("button", {})
                        .get("buttonRenderer", {})
                        .get("command", {})
                        .get("continuationCommand", {})
                        .get("token")
                    )

    return replies


def extract_comment_continuation_token(data: dict) -> str:
    # Đường 1: onResponseReceivedEndpoints
    try:
        for ep in data.get("onResponseReceivedEndpoints", []):
            for action_key in (
                "reloadContinuationItemsCommand",
                "appendContinuationItemsAction",
            ):
                for item in ep.get(action_key, {}).get("continuationItems", []):
                    token = (
                        item.get("continuationItemRenderer", {})
                        .get("continuationEndpoint", {})
                        .get("continuationCommand", {})
                        .get("token")
                    )
                    if token:
                        return token
    except Exception as e:
        logger.debug("Đường 1 (onResponseReceivedEndpoints) lỗi: %s", e)

    # Đường 2: twoColumnWatchNextResults
    try:
        results = (
            data.get("contents", {})
            .get("twoColumnWatchNextResults", {})
            .get("results", {})
            .get("results", {})
            .get("contents", [])
        )
        for item in results:
            for content in item.get("itemSectionRenderer", {}).get("contents", []):
                token = (
                    content.get("continuationItemRenderer", {})
                    .get("continuationEndpoint", {})
                    .get("continuationCommand", {})
                    .get("token")
                )
                if token:
                    return token
    except Exception as e:
        logger.debug("Đường 2 (twoColumnWatchNextResults) lỗi: %s", e)

    # Đường 3: engagementPanels
    try:
        for panel in data.get("engagementPanels", []):
            renderer = panel.get("engagementPanelSectionListRenderer", {})
            if "comment" not in renderer.get("panelIdentifier", "").lower():
                continue
            section = renderer.get("content", {}).get("sectionListRenderer", {})
            for cont in section.get("continuations", []):
                token = cont.get("nextContinuationData", {}).get("continuation")
                if token:
                    return token
    except Exception as e:
        logger.debug("Đường 3 (engagementPanels) lỗi: %s", e)

    # Đường 4: mutations trong frameworkUpdates
    try:
        for m in data.get("frameworkUpdates", {}).get("entityBatchUpdate", {}).get("mutations", []):
            token = m.get("payload", {}).get("continuationEndpoint", {}).get("continuationCommand", {}).get("token")
            if token:
                return token
    except Exception as e:
        logger.debug("Đường 4 (frameworkUpdates) lỗi: %s", e)

    return None


def parse_comment_entities(data: dict) -> dict:
    result = {}
    for m in data.get("frameworkUpdates", {}).get("entityBatchUpdate", {}).get("mutations", []):
        payload = m.get("payload", {})
        comment = payload.get("commentEntityPayload", {})
        props = comment.get("properties", {})
        comment_id = props.get("commentId")
        raw_content = props.get("content", {}).get("content", "")
        if not isinstance(raw_content, str):
            continue
        if comment_id:
            result[comment_id] = {
                "content": raw_content,
                "author": comment.get("author", {}).get("displayName", ""),
                "avatar": comment.get("author", {}).get("avatarThumbnailUrl", ""),
                "published_time": props.get("publishedTime", "Unknown"),
                "likes": parse_view_count(comment.get("toolbar", {}).get("likeCountLiked")),
                "replies": parse_view_count(comment.get("toolbar", {}).get("replyCount")),
            }
    return result


async def get_video_comments(
    video_id: str,
    proxy: str = None,
    max_comments: int = 100,
    sort: Literal["top", "newest"] = "top",
    reply_pages: int = 2,
    reply_concurrency: int = 8,
) -> list[dict]:
    """reply_pages: số trang trả lời tối đa mỗi bình luận (data-miner cố định 2 trang ~ 20 trả lời đầu)."""
    api_key = await get_youtube_api_key(proxy=proxy)
    url_next = get_youtube_api_url(ENDPOINT_NEXT, api_key)
    context = get_context()
    comments = []

    async with create_httpx_client(proxy=proxy) as client:
        if sort == "newest":
            # Token protobuf dựng sẵn dùng ổn cho sắp xếp mới nhất.
            continuation_token = _build_comment_sort_token(video_id, "newest")
            logger.debug("Dùng token sắp xếp mới nhất dựng sẵn cho %s", video_id)
        else:
            # sort="top" cần session token từ YouTube ; phải lấy trước.
            resp = await client.post(url_next, json={"context": context, "videoId": video_id})
            resp.raise_for_status()
            data = resp.json()
            continuation_token = extract_comment_continuation_token(data)
            if not continuation_token:
                logger.warning(
                    "[comments] no continuation token for %s ; comments may be disabled",
                    video_id,
                )
                return []

        while continuation_token and len(comments) < max_comments:
            resp = await client.post(url_next, json={"context": context, "continuation": continuation_token})
            resp.raise_for_status()
            data = resp.json()
            entity_map = parse_comment_entities(data)
            continuation_token = None

            for action in data.get("onResponseReceivedEndpoints", []):
                items = action.get("reloadContinuationItemsCommand", {}).get("continuationItems", []) or action.get(
                    "appendContinuationItemsAction", {}
                ).get("continuationItems", [])
                for item in items:
                    if "commentThreadRenderer" in item:
                        thread = item["commentThreadRenderer"]
                        comment_vm = thread.get("commentViewModel", {}).get("commentViewModel", {})
                        comment_id = comment_vm.get("commentId")
                        entity = entity_map.get(comment_id, {})
                        if not entity:
                            continue
                        content = entity.get("content", "")
                        if not isinstance(content, str):
                            continue

                        comment_data = {
                            "comment_id": comment_id,
                            "author": entity.get("author", ""),
                            "avatar": entity.get("avatar"),
                            "content": content,
                            "published_time": entity.get("published_time", ""),
                            "likes": entity.get("likes", 0),
                            "replies_count": entity.get("replies", 0),
                            "replies": [],
                        }

                        reply_token = None
                        for c in thread.get("replies", {}).get("commentRepliesRenderer", {}).get("contents", []):
                            continuation = (
                                c.get("continuationItemRenderer", {})
                                .get("continuationEndpoint", {})
                                .get("continuationCommand", {})
                                .get("token")
                            )
                            if continuation:
                                reply_token = continuation
                                break

                        comments.append(comment_data)
                        if reply_token:
                            comment_data["_reply_token"] = reply_token
                        if len(comments) >= max_comments:
                            break

                    elif "continuationItemRenderer" in item:
                        continuation_token = (
                            item.get("continuationItemRenderer", {})
                            .get("continuationEndpoint", {})
                            .get("continuationCommand", {})
                            .get("token")
                        )

                if len(comments) >= max_comments:
                    break

        # Lấy song song mọi phản hồi thay vì await lần lượt
        pending = [(i, c.pop("_reply_token")) for i, c in enumerate(comments) if "_reply_token" in c]
        if pending:
            logger.debug(
                "[comments] fetching replies for %d comments in parallel",
                len(pending),
            )
            reply_sem = asyncio.Semaphore(reply_concurrency)

            async def _replies(tok: str):
                async with reply_sem:
                    return await fetch_replies(client, tok, context, proxy=proxy, max_depth=reply_pages)

            results = await asyncio.gather(*[_replies(tok) for _, tok in pending], return_exceptions=True)
            for (idx, _), result in zip(pending, results):
                if isinstance(result, list):
                    comments[idx]["replies"] = result

    return comments[:max_comments]


async def get_video_comments_batch(
    video_ids: list[str],
    proxy: str = None,
    max_per_video: int = 20,
    sort: Literal["top", "newest"] = "top",
    concurrency: int = 3,
) -> dict:
    """
    Lấy bình luận của nhiều video song song (giới hạn bởi `concurrency`).
    Video tắt/không có bình luận thì bỏ qua ; trả về những video có bình luận.
    Đi cùng việc chọn N video nhiều view nhất để agent không bao giờ kẹt
    ở một video tắt bình luận.
    """
    sem = asyncio.Semaphore(concurrency)

    async def _one(vid: str):
        async with sem:
            try:
                comments = await get_video_comments(vid, proxy=proxy, max_comments=max_per_video, sort=sort)
                return vid, comments, None
            except Exception as e:
                return vid, [], str(e)

    results = await asyncio.gather(*[_one(v) for v in video_ids])

    per_video = []
    skipped = []
    for vid, comments, err in results:
        if err:
            skipped.append({"video_id": vid, "reason": "error", "detail": err})
        elif not comments:
            skipped.append({"video_id": vid, "reason": "disabled_or_empty"})
        else:
            per_video.append({"video_id": vid, "total": len(comments), "comments": comments})

    total = sum(v["total"] for v in per_video)
    logger.info(
        "[comments/batch] %d id -> %d có comments, %d bỏ qua, tổng %d comments",
        len(video_ids),
        len(per_video),
        len(skipped),
        total,
    )
    return {
        "requested": len(video_ids),
        "videos_with_comments": len(per_video),
        "videos_skipped": len(skipped),
        "total_comments": total,
        "results": per_video,
        "skipped": skipped,
    }
