"""social_crawler/spiders/post_log.py - một dòng log cho mỗi bài crawl được."""

from __future__ import annotations

from social_crawler.spiders.post_log import log_crawled_post, preview


class _Recorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def info(self, event: str, **fields) -> None:
        self.calls.append((event, fields))


def test_preview_flattens_and_truncates() -> None:
    assert preview("Phim hay\n\n  quá   đi") == "Phim hay quá đi"
    assert preview(None) == ""
    long = preview("a" * 300, limit=20)
    assert len(long) == 20 and long.endswith("…")


def test_log_crawled_post_drops_missing_stats() -> None:
    logger = _Recorder()
    log_crawled_post(
        logger,
        platform="facebook",
        post_id="p1",
        text="Người Được Chọn\nkhởi chiếu 06.11",
        author="Cinemark",
        url="https://facebook.com/p1",
        likes=12,
        comments=None,
        shares=0,
    )
    event, fields = logger.calls[0]
    assert event == "post_crawled"
    assert fields == {
        "platform": "facebook",
        "post_id": "p1",
        "author": "Cinemark",
        "text": "Người Được Chọn khởi chiếu 06.11",
        "likes": 12,
        "shares": 0,
        "url": "https://facebook.com/p1",
    }
