import asyncio

from social_crawler.spiders.youtube._compat import Logger
from social_crawler.spiders.youtube._compat import get_proxy

logger = Logger.get(__name__)

_LANG_PRIORITY = ["vi", "en", "a.vi", "a.en"]


def _fetch_sync(video_id: str, proxy_url: str | None = None) -> dict | None:
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
        from youtube_transcript_api._errors import NoTranscriptFound, TranscriptsDisabled

        proxy_config = None
        if proxy_url:
            from youtube_transcript_api.proxies import GenericProxyConfig

            proxy_config = GenericProxyConfig(http_url=proxy_url, https_url=proxy_url)
        api = YouTubeTranscriptApi(proxy_config=proxy_config)

        try:
            transcript_list = api.list(video_id)
        except TranscriptsDisabled:
            logger.info("[transcript] %s tắt phụ đề", video_id)
            return None

        for lang in _LANG_PRIORITY:
            try:
                transcript = transcript_list.find_transcript([lang])
                segments = transcript.fetch()
                text = " ".join(s.text for s in segments).strip()
                return {
                    "video_id": video_id,
                    "language": transcript.language_code,
                    "text": text,
                    "char_count": len(text),
                    "segments": len(segments),
                }
            except NoTranscriptFound, Exception:
                continue

        try:
            first = next(iter(transcript_list))
            segments = first.fetch()
            text = " ".join(s.text for s in segments).strip()
            return {
                "video_id": video_id,
                "language": first.language_code,
                "text": text,
                "char_count": len(text),
                "segments": len(segments),
            }
        except Exception:
            return None

    except Exception as e:
        logger.warning("[transcript] %s ; %s", video_id, e)
        return None


async def get_transcript(video_id: str) -> dict | None:
    proxy_url = await get_proxy()
    result = await asyncio.to_thread(_fetch_sync, video_id, proxy_url)
    if result:
        logger.info(
            "[transcript] %s lang=%s chars=%d",
            video_id,
            result["language"],
            result["char_count"],
        )
    else:
        logger.warning("[transcript] %s ; không có phụ đề", video_id)
    return result


async def get_transcript_batch(
    video_ids: list[str],
    concurrency: int = 3,
) -> dict[str, dict | None]:
    sem = asyncio.Semaphore(concurrency)

    async def _one(vid: str):
        async with sem:
            return vid, await get_transcript(vid)

    pairs = await asyncio.gather(*[_one(v) for v in video_ids])
    results = {vid: data for vid, data in pairs}
    found = sum(1 for v in results.values() if v)
    logger.info("[transcript/batch] %d/%d video có phụ đề", found, len(video_ids))
    return results
