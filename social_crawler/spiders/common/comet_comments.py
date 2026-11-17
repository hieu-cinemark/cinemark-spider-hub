"""Lấy bình luận và reply của client GraphQL Comet (mixin).

Tách từ comet_graphql_client.py (17/11/2026).
"""

from __future__ import annotations

from typing import Any

from social_crawler.core.logger import get_logger

logger = get_logger(__name__)

from social_crawler.spiders.common.comet_errors import SessionExpiredError


class CommentsMixin:
    """Lấy bình luận/reply qua GraphQL bằng công thức đã bootstrap. Trộn vào CometGraphQLClient."""

    def _comment_target_id(self, post_id: str) -> str:
        """Cách query comment của nền tảng này định địa chỉ một bài - Facebook dùng
        base64("feedback:<post_id>") (xem FacebookGraphQLClient), lớp con có tính năng comment
        phải ghi đè bằng cách định địa chỉ riêng đã xác nhận với request thật."""
        raise NotImplementedError(f"{self.PLATFORM} has no comments feature (no _comment_target_id override)")

    def _reply_target_id(self, legacy_comment_id: str) -> str:
        """Cách query reply-của-comment của nền tảng này định địa chỉ comment cha. Mặc định dùng
        cùng cách với _comment_target_id (cách định địa chỉ base64("feedback:<id>") của
        Facebook, đã xác nhận cho bài - CHƯA được xác nhận độc lập cho id comment; lớp con nên
        ghi đè khi một request reply thật bắt được cho thấy khác)."""
        return self._comment_target_id(legacy_comment_id)

    def _get_comments_cache(self) -> dict[str, Any]:
        comments_key = self.COMMENTS_REDIS_KEY_TMPL.format(account=self._account)
        comments = self._redis.get(comments_key)
        if comments is None:
            raise SessionExpiredError(
                f"No comments query cached in Redis (key={comments_key!r}, account={self._account!r}). Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --post-url "<a post url with comments>"'
            )
        return comments

    def get_comments(self, post_id: str) -> dict[str, Any]:
        """Lấy trang comment đầu tiên của một bài - dùng chung cho mọi nền tảng có tính năng
        comment (xem _comment_target_id).

        Reset rõ cursor về null dù đây là query "ban đầu", không phải query "phân trang": trên
        nền tảng mà cùng một query refetch đảm nhận cả hai vai (đã xác nhận trên Threads - xem
        pick_paginated_comments_request trong request_capture.py), request bắt được giữa lúc
        cuộn trong bootstrap đã mang sẵn một giá trị cursor thật (giờ đã cũ) trong mẫu. Để
        nguyên thì trang 1 sẽ âm thầm hỏi "những gì sau cursor cũ đó" thay vì trang đầu thật, và
        nhận về direct_replies rỗng. Vô hại trên nền tảng có query comment gốc/phân trang thực sự
        tách riêng (Facebook): mẫu đó đơn giản là không có key cursor nào để lượt duyệt này đụng
        tới."""
        comments = self._get_comments_cache()
        return self._run(
            doc_id=comments.get("doc_id"),
            friendly_name=comments.get("fb_api_req_friendly_name"),
            template=comments.get("variables_template"),
            template_source="comments variables_template",
            overrides={self.COMMENTS_ID_KEY: self._comment_target_id(post_id), self.COMMENTS_CURSOR_KEY: None},
        )

    def get_comments_next_page(self, post_id: str, cursor: str, count: int = -1) -> dict[str, Any]:
        """Lấy trang comment kế tiếp, dùng `end_cursor` từ `page_info` của trang trước (xem
        `find_page_info`). Cần bootstrap.py đã bắt được một request comment có phân trang - nó
        tự làm việc này bằng cách cuộn danh sách comment sau khi đổi thứ tự sắp xếp (xem
        comments_trigger riêng của từng nền tảng).

        Mặc định count=-1 khớp với CommentsListComponentsPaginationQuery của chính Comet (đã xác
        nhận thực tế 2026-09-16): kích thước trang dương vẫn bị giới hạn khoảng 10; -1 là thứ
        trình duyệt gửi để lấy trang dày nhất sau cursor."""
        comments = self._get_comments_cache()
        pagination = comments.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no comments pagination info (no paginated comments query was captured). "
                "Re-run bootstrap.py --post-url against a post with more comments than fit on one page."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="comments pagination.variables_template",
            overrides={
                self.COMMENTS_ID_KEY: self._comment_target_id(post_id),
                self.COMMENTS_CURSOR_KEY: cursor,
                self.COMMENTS_COUNT_KEY: count,
            },
        )

    def _get_replies_cache(self) -> dict[str, Any]:
        replies_key = self.REPLIES_REDIS_KEY_TMPL.format(account=self._account)
        replies = self._redis.get(replies_key)
        if replies is None:
            raise SessionExpiredError(
                f"No replies query cached in Redis (key={replies_key!r}, account={self._account!r}). Run this first:\n"
                f'  python -m social_crawler.spiders.{self.PLATFORM}.auth.bootstrap --post-url "<a post url whose top-level '
                'comment has replies>" --type replies'
            )
        return replies

    def get_replies(self, legacy_comment_id: str) -> dict[str, Any]:
        """Lấy trang reply đầu tiên của một comment cấp một - giống get_comments ở trên, chỉ là định
        địa chỉ vào một comment thay vì một bài (xem _reply_target_id) và cache dưới
        REPLIES_REDIS_KEY_TMPL."""
        replies = self._get_replies_cache()
        return self._run(
            doc_id=replies.get("doc_id"),
            friendly_name=replies.get("fb_api_req_friendly_name"),
            template=replies.get("variables_template"),
            template_source="replies variables_template",
            overrides={self.COMMENTS_ID_KEY: self._reply_target_id(legacy_comment_id), self.COMMENTS_CURSOR_KEY: None},
        )

    def get_replies_next_page(self, legacy_comment_id: str, cursor: str, count: int = 10) -> dict[str, Any]:
        """Lấy trang reply kế tiếp, dùng `end_cursor` từ `page_info` của trang trước (xem
        `find_page_info`). Giống get_comments_next_page ở trên."""
        replies = self._get_replies_cache()
        pagination = replies.get("pagination")
        if pagination is None:
            raise SessionExpiredError(
                "Cache has no replies pagination info (no paginated replies query was captured). "
                "Re-run bootstrap.py --post-url ... --type replies against a comment with more replies than fit on one page."
            )
        return self._run(
            doc_id=pagination.get("doc_id"),
            friendly_name=pagination.get("fb_api_req_friendly_name"),
            template=pagination.get("variables_template"),
            template_source="replies pagination.variables_template",
            overrides={
                self.COMMENTS_ID_KEY: self._reply_target_id(legacy_comment_id),
                self.COMMENTS_CURSOR_KEY: cursor,
                self.COMMENTS_COUNT_KEY: count,
            },
        )
