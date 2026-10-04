"""
Biến một response /api/post/item_list/ thô của TikTok thành các bản ghi video phẳng, dễ
phân tích. Đã xác nhận với một response thật bắt được (xem docstring module của
features/channel_videos/search.py): dạng cấp cao nhất (itemList/cursor/hasMore/...) và các
trường riêng của mỗi item (id/desc/video/author/stats/contents/challenges/...) giống từng
byte với response /api/challenge/item_list/ của hashtag_search, nên
extract_response/extract_video được dùng lại nguyên ở đây thay vì viết lại. Response của
endpoint này hoàn toàn không cần lớp chuyển đổi nào.
"""

from __future__ import annotations

from social_crawler.spiders.tiktok.features.hashtag_search.extract import (
    extract_response,
    extract_video,
)

__all__ = ["extract_response", "extract_video"]
