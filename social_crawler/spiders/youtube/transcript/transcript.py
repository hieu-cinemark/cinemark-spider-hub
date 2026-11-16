"""Transcript (phụ đề) một video YouTube qua youtube-transcript-api (16/11/2026; logic chép từ data-miner).

Ưu tiên phụ đề tiếng Việt rồi tiếng Anh (gồm phụ đề tự động), không có thì lấy phụ đề đầu tiên. Thử 16/11/2026: 6/6 video
tiếng Việt có phụ đề, cả từ IP VPS. Trả None khi video không có / tắt phụ đề; lỗi mạng / bị chặn thì raise để chỗ gọi lấy lại
lần sau (consumer/jobs/youtube.py không đánh dấu "đã lấy").
"""

from __future__ import annotations

import asyncio
from typing import Any

from social_crawler.spiders.youtube._compat import Logger

logger = Logger.get(__name__)

_LANG_PRIORITY = ["vi", "en"]
# Video dài (livestream, truyện audio) có thể hàng trăm nghìn ký tự - giữ đủ để phân tích, không làm phình message Kafka.
MAX_CHARS = 30000


def _pick(transcript_list: Any) -> Any:
    from youtube_transcript_api._errors import NoTranscriptFound

    try:
        return transcript_list.find_transcript(_LANG_PRIORITY)
    except NoTranscriptFound:
        return next(iter(transcript_list), None)


def _fetch_sync(video_id: str, proxy_url: str | None) -> dict[str, Any] | None:
    from youtube_transcript_api import YouTubeTranscriptApi
    from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled, VideoUnavailable

    proxy_config = None
    if proxy_url:
        from youtube_transcript_api.proxies import GenericProxyConfig

        proxy_config = GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
    api = YouTubeTranscriptApi(proxy_config=proxy_config)
    try:
        chosen = _pick(api.list(video_id))
        if chosen is None:
            return None
        segments = chosen.fetch()
    except TranscriptsDisabled, NoTranscriptFound, VideoUnavailable:
        return None
    text = " ".join(" ".join(s.text.split()) for s in segments).strip()
    if not text:
        return None
    return {
        "language": chosen.language_code,
        "generated": bool(chosen.is_generated),
        "text": text[:MAX_CHARS],
        "char_count": len(text),
    }


async def get_transcript(video_id: str, proxy: str | None = None) -> dict[str, Any] | None:
    """{language, generated, text (tối đa MAX_CHARS), char_count (độ dài đầy đủ)} hoặc None khi video không có phụ đề."""
    return await asyncio.to_thread(_fetch_sync, video_id, proxy)
