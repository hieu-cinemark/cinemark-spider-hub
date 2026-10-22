"""Job tìm kiếm theo từ khoá / hashtag (type mặc định)."""

from __future__ import annotations

import json
import os
import time
import zlib
from datetime import date
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import (
    CRAWL_JOB_CANCEL_KEY_TMPL,
    CRAWL_JOB_KEY_TMPL,
    SCRAPY_BIN,
    SPIDER_BY_PLATFORM,
    TIKTOK_HASHTAG_SPIDER,
    TIKTOK_KEYWORD_SPIDER,
    CrawlJobFailed,
    CrawlJobSkipped,
    _raise_for_returncode,
    logger,
)
from social_crawler.consumer.process import _run_subprocess
from social_crawler.consumer.sessions import _ensure_facebook_session, _ensure_threads_session

DEFAULT_SWEEP_DAYS = 60  # khoảng 2 tháng


# Lượt quét hằng ngày chỉ phủ chừng này ngày gần nhất; đủ DEFAULT_SWEEP_DAYS chỉ một ngày mỗi tuần cho mỗi từ khoá (lệch ngày
# theo keyword_id để rải tải). 19/10/2026: quét 60 ngày (16 khoảng) cho 150 từ khoá MỖI NGÀY là phần lớn request của tài
# khoản FB, mà bài cũ hơn ~1 tuần gần như đã cào hết - số liệu của bài cũ do lượt comment/chụp số liệu cập nhật.
DAILY_SWEEP_DAYS = int(os.getenv("FACEBOOK_DAILY_SWEEP_DAYS", "14"))


FULL_SWEEP_EVERY_DAYS = 7


def _sweep_days_for(request: dict[str, Any]) -> int:
    """facebook_search nên quét bao nhiêu ngày. Spider neo các khoảng thời gian ở end_date
    (hoặc hôm nay) và lùi lại sweep_days (xem _date_windows). Một khoảng ngày người dùng chọn
    được quy ra số ngày đó thay vì một query không lọc duy nhất mà Facebook sẽ giới hạn.
    Không có khoảng ngày - DEFAULT_SWEEP_DAYS."""
    start_raw, end_raw = request.get("start_date"), request.get("end_date")
    if start_raw and end_raw:
        start, end = date.fromisoformat(start_raw), date.fromisoformat(end_raw)
        return max((end - start).days + 1, 1)
    if start_raw:
        return max((date.today() - date.fromisoformat(start_raw)).days + 1, 1)
    keyword_id = str(request.get("keyword_id") or "")
    if not keyword_id or DAILY_SWEEP_DAYS <= 0 or DAILY_SWEEP_DAYS >= DEFAULT_SWEEP_DAYS:
        return DEFAULT_SWEEP_DAYS
    full_turn = (date.today().toordinal() + zlib.crc32(keyword_id.encode())) % FULL_SWEEP_EVERY_DAYS == 0
    return DEFAULT_SWEEP_DAYS if full_turn else DAILY_SWEEP_DAYS


async def _run_spider(request: dict[str, Any]) -> None:
    platform = request.get("platform")

    # Một lần bấm Dừng bật bfs_drain:<platform> trong Redis (xem crawl_jobs.request_stop của
    # cinemark-api) chính là để các lượt crawl tìm ra qua BFS đã xếp hàng sau thứ nó huỷ không
    # cứ thế chạy tiếp từng cái một - chỉ request có bfs_depth mới bị drain (bfs_depth không
    # bao giờ được đặt trên request thật kích hoạt từ dashboard/cron), nên một lượt crawl xếp
    # hàng bằng tay cho từ khoá khác vẫn chạy bình thường kể cả khi drain đang bật.
    if request.get("bfs_depth") is not None and RedisCache().exists(f"bfs_drain:{platform}"):
        logger.info("bfs_request_skipped_drain", platform=platform, keyword=request.get("keyword"))
        raise CrawlJobSkipped("bfs drain")

    keyword = request.get("keyword")
    if not keyword:
        logger.warning("crawl_request_missing_keyword", request=request)
        raise CrawlJobFailed("crawl request missing keyword")

    if platform not in ("facebook", "threads", "tiktok"):
        logger.warning("unsupported_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported platform {platform}")

    # Threads không cào hashtag (2026-10-15): tag ít bài thì Threads trả bài ngẫu nhiên toàn cầu - đo 01-15/10 ~1% liên quan.
    # Chặn cả request đã xếp hàng trước khi cinemark-api/dashboard bỏ hashtag Threads.
    if platform == "threads" and str(keyword).strip().startswith("#"):
        logger.info("threads_hashtag_skipped", keyword=keyword, keyword_id=request.get("keyword_id"))
        raise CrawlJobSkipped("threads hashtag search disabled")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "keyword": keyword,
                "keyword_id": request.get("keyword_id"),
                "started_at": int(time.time()),
            },
        )

    if platform == "tiktok":
        # "#tag" -> feed hashtag; text tự do -> tìm kiếm từ khoá (search/general/full, có từ
        # 2026-10-13 - trước đó text tự do bị skip vì TikTok chỉ crawl được hashtag).
        if str(keyword).startswith("#"):
            args = [SCRAPY_BIN, "crawl", TIKTOK_HASHTAG_SPIDER, "-a", f"hashtag={keyword.lstrip('#')}"]
        else:
            args = [SCRAPY_BIN, "crawl", TIKTOK_KEYWORD_SPIDER, "-a", f"keyword={keyword}"]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
        if request.get("bfs_depth"):
            args += ["-a", f"bfs_depth={request['bfs_depth']}"]
        if request.get("movie_context"):
            args += ["-a", f"movie_context={request['movie_context']}"]
    else:
        args = [
            SCRAPY_BIN,
            "crawl",
            SPIDER_BY_PLATFORM[platform],
            "-a",
            f"query={keyword}",
            "-a",
            "include_entities=false",
        ]
        if request.get("keyword_id"):
            args += ["-a", f"keyword_id={request['keyword_id']}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]
        # Tên phim cho bộ lọc lạc đề (spiders/keyword_filter.py) - bài nhắc tên phim được giữ dù không chứa đúng câu tìm.
        if request.get("match_terms"):
            args += ["-a", f"match_terms={json.dumps(request['match_terms'], ensure_ascii=False)}"]
        # Khoảng ngày chỉ có ở Facebook (Threads không có bộ lọc ngày đăng).
        if platform == "facebook":
            if request.get("start_date"):
                args += ["-a", f"start_date={request['start_date']}"]
            if request.get("end_date"):
                args += ["-a", f"end_date={request['end_date']}"]
            if request.get("recent_post"):
                args += ["-a", "recent_post=true"]
            # recent_post không kèm khoảng ngày: một lượt duy nhất, mới nhất trước, sâu tới max_pages -
            # quét theo khoảng mặc định sẽ cắt nó thành các cửa sổ ngày và bỏ phí thứ tự "mới nhất".
            recent_only = request.get("recent_post") and not (request.get("start_date") or request.get("end_date"))
            args += ["-a", f"sweep_days={0 if recent_only else _sweep_days_for(request)}"]
        else:
            if request.get("recent_post"):
                args += ["-a", "recent=true"]  # Threads: tab "Gần đây"
            if request.get("start_date") or request.get("end_date"):
                logger.info(
                    "crawl_request_dates_dropped",
                    platform=platform,
                    keyword=keyword,
                    keyword_id=request.get("keyword_id"),
                    start_date=request.get("start_date"),
                    end_date=request.get("end_date"),
                )

    # Được publish_crawl_request của cinemark-api đặt cho mọi lượt chạy kích hoạt từ dashboard,
    # và cho các bước nhảy BFS đã được người vận hành duyệt (chip hashtag liên quan). Thứ gì
    # đang chạy cho nền tảng này là thứ nút Dừng trên dashboard huỷ (xem crawl_jobs.py).
    logger.info(
        "crawl_request_started", platform=platform, keyword=keyword, keyword_id=request.get("keyword_id"), run_id=run_id
    )
    try:
        if platform == "facebook" and not await _ensure_facebook_session(run_id=run_id, platform=platform):
            raise CrawlJobFailed("facebook session refresh failed before search crawl")
        if platform == "threads" and not await _ensure_threads_session(run_id=run_id, platform=platform):
            raise CrawlJobFailed("threads session refresh failed before search crawl")
        returncode = await _run_subprocess(args, run_id=run_id, platform=platform)
        _raise_for_returncode(returncode, f"{platform} crawl keyword={keyword}")
        logger.info("crawl_request_finished", platform=platform, keyword=keyword)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))
