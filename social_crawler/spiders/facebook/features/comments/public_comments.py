"""Bình luận Facebook chỉ công khai và phản hồi tầng 1 có phân trang, giữ hợp đồng Kafka hiện có."""

from __future__ import annotations

import asyncio

import scrapy

from social_crawler.clients.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.core.logger import get_logger
from social_crawler.spiders.facebook.auth.public_comments import (
    PublicCommentsClient,
    PublicCommentsError,
    numeric_post_id,
)
from social_crawler.spiders.facebook.features.comments.extract import comment_post_id, extract_comment
from social_crawler.spiders.facebook.items import FacebookCommentItem

logger = get_logger(__name__)


class FacebookPublicCommentsSpider(scrapy.Spider):
    name = "facebook_public_comments"

    def __init__(
        self, post_id=None, post_url=None, max_pages=0, include_replies="true", proxy_id=None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.post_id = str(post_id)
        self.target_post_id = numeric_post_id(post_id)
        self.post_url = post_url
        self.max_pages = int(max_pages)
        if self.max_pages < 0:
            raise ValueError("max_pages must be nonnegative")
        self.include_replies = str(include_replies).lower() not in ("false", "0", "no", "")
        self.proxy_id = proxy_id
        self._kafka = KafkaPublisher()
        self._seen = set()
        self._client = None

    async def _emit(self, node, parent_id=None):
        comment = extract_comment(node)
        comment.pop("reply_expansion_token", None)
        identifier = comment.get("comment_id")
        if not identifier:
            raise PublicCommentsError("Public comment has no stable GraphQL ID")
        owner = comment_post_id(identifier)
        if owner and owner != self.target_post_id:
            raise PublicCommentsError("Public query returned a comment from a different post")
        if identifier in self._seen:
            return None
        body = {"platform": "facebook", "post_id": self.post_id, **comment}
        if parent_id:
            body["parent_comment_id"] = parent_id
        delivered = await self._kafka.publish(topic=RAW_COMMENTS_TOPIC, key=f"facebook:{identifier}", value=body)
        if not delivered:
            raise PublicCommentsError("Comment was not acknowledged by Kafka; crawl is incomplete")
        # Chỉ đánh dấu sau khi broker xác nhận. Job sau có thể làm mới cùng bình luận.
        self._seen.add(identifier)
        return FacebookCommentItem(**{k: v for k, v in body.items() if k != "platform"})

    async def _replies(self, parent):
        feedback = parent.get("feedback") or {}
        identifier = feedback.get("id")
        expansion = (feedback.get("expansion_info") or {}).get("expansion_token")
        reply_count = (feedback.get("replies_fields") or {}).get("total_count")
        if reply_count == 0:
            return
        if not identifier or not expansion:
            if reply_count:
                raise PublicCommentsError("Reply metadata is missing; top-level comments were retained")
            return
        cursor = None
        seen_cursors = set()
        page = 0
        while True:
            nodes, info = await asyncio.to_thread(self._client.replies, identifier, expansion, cursor)
            for node in nodes:
                item = await self._emit(node, parent["id"])
                if item is not None:
                    yield item
            page += 1
            if not info["has_next_page"]:
                return
            cursor = info["end_cursor"]
            if cursor in seen_cursors:
                raise PublicCommentsError("Reply cursor repeated; crawl is incomplete")
            seen_cursors.add(cursor)
            if self.max_pages and page >= self.max_pages:
                # max_pages dương là độ sâu job chọn, không phải lỗi; coi là lỗi sẽ xếp lại bài từ trang 1.
                logger.info("public_replies_page_limit_reached", post_id=self.post_id, pages=page)
                return

    async def start(self):
        await self._kafka.start()
        try:
            self._client = await asyncio.to_thread(PublicCommentsClient, proxy_id=self.proxy_id)
            cursor = None
            seen_cursors = set()
            page = 0
            while True:
                nodes, info = await asyncio.to_thread(self._client.comments, self.target_post_id, cursor)
                for node in nodes:
                    item = await self._emit(node)
                    if item is not None:
                        yield item
                    if self.include_replies:
                        async for reply in self._replies(node):
                            yield reply
                page += 1
                logger.info(
                    "public_comments_page",
                    post_id=self.post_id,
                    page=page,
                    fetched=len(nodes),
                    published=len(self._seen),
                )
                if not info["has_next_page"]:
                    break
                cursor = info["end_cursor"]
                if cursor in seen_cursors:
                    raise PublicCommentsError("Comment cursor repeated; crawl is incomplete")
                seen_cursors.add(cursor)
                if self.max_pages and page >= self.max_pages:
                    logger.info("public_comments_page_limit_reached", post_id=self.post_id, pages=page)
                    break
            logger.info(
                "public_comments_finished",
                post_id=self.post_id,
                pages=page,
                comments=len(self._seen),
                access_mode="anonymous",
            )
        finally:
            if self._client:
                await asyncio.to_thread(self._client.close)
            await self._kafka.stop()
