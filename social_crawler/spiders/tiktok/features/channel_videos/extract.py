"""
Turns a raw TikTok /api/post/item_list/ response into flat, analysis-
friendly video records. Confirmed against a real captured response (see
features/channel_videos/search.py's module docstring): the top-level shape
(itemList/cursor/hasMore/...) and each item's own fields (id/desc/video/
author/stats/contents/challenges/...) are byte-for-byte the same shape as
hashtag_search's /api/challenge/item_list/ response, so extract_response/
extract_video are reused as-is here rather than re-implemented. This
endpoint's response needs no adapter at all.
"""

from __future__ import annotations

from social_crawler.spiders.tiktok.features.hashtag_search.extract import (
    extract_response,
    extract_video,
)

__all__ = ["extract_response", "extract_video"]
