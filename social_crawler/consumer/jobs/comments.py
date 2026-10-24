"""Job cào bình luận của một bài (type="comments")."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.facebook import (
    ACTIVE_ACCOUNT_REDIS_KEY,
)
from social_crawler.consumer.common import (
    COMMENTS_PLATFORMS_NEEDING_CACHE,
    COMMENTS_SPIDER_BY_PLATFORM,
    CRAWL_JOB_CANCEL_KEY_TMPL,
    CRAWL_JOB_KEY_TMPL,
    SCRAPY_BIN,
    CrawlJobFailed,
    CrawlJobSkipped,
    _raise_for_returncode,
    logger,
)
from social_crawler.consumer.process import _run_subprocess, _stopped_for
from social_crawler.consumer.sessions import (
    _ensure_comments_cache,
    _ensure_facebook_session,
    _ensure_replies_cache,
    _facebook_session_is_cached,
)
from social_crawler.services import lanes


async def _run_comments_spider(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Xử lý request type="comments" (xem publish_comments_crawl_request của cinemark-api) -
    chạy spider comment của nền tảng đích cho đúng một bài (xem COMMENTS_SPIDER_BY_PLATFORM;
    nền tảng không có trong đó thì hoàn toàn chưa có tính năng comment - xem docstring
    get_comment_mapper phía cinemark-api). Dùng chung crawl_job:<platform> với các lượt crawl
    tìm kiếm của _run_spider, đây là chủ đích, không phải sơ suất - cả hai đi qua cùng vòng
    lặp consumer từng-cái-một theo nền tảng (xem docstring module), nên một lượt crawl
    comment và một lượt crawl tìm kiếm của cùng nền tảng vốn không bao giờ chạy đồng thời
    trên cùng tài khoản/session."""
    platform = request.get("platform", "facebook")
    spider_name = COMMENTS_SPIDER_BY_PLATFORM.get(platform)
    if spider_name is None:
        logger.warning("unsupported_comments_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported comments platform {platform}")

    post_id = request.get("post_id")
    post_url = request.get("post_url")
    if not post_id or not post_url:
        logger.warning("comments_request_missing_fields", request=request)
        raise CrawlJobFailed("comments request missing post_id or post_url")

    # Báo job *trước khi* ensure_*/bootstrap để Dừng huỷ được việc bắt cache comment kéo dài -
    # trước đây crawl_job chỉ được đặt khi spider scrapy bắt đầu, nên lần Dừng đầu tiên trong
    # lúc bootstrap chỉ bật drain mà không kill được gì, và lượt crawl vẫn chạy tiếp.
    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = CRAWL_JOB_KEY_TMPL.format(platform=platform)
    if run_id:
        cache.set(
            job_key,
            {
                "run_id": run_id,
                "type": "comments",
                "post_id": post_id,
                "started_at": int(time.time()),
            },
        )

    def _stopped() -> bool:
        return _stopped_for(run_id=run_id, platform=platform, bypass_drain=bypass_drain)

    returncode: int | None = None
    try:
        # Ghim một tài khoản cho mọi lời gọi ensure_* bên dưới thay vì để mỗi lời gọi tự đọc/xoay
        # ACTIVE_ACCOUNT_REDIS_KEY - xem docstring của _facebook_replies_cache_usable cho lỗi mà
        # việc này sửa. Bắt đầu bằng một phỏng đoán tốt nhất (tài khoản nào đang active);
        # _ensure_facebook_session có thể bootstrap một tài khoản *khác* nếu phỏng đoán đó hoá ra
        # đã cũ (chưa có dòng tài khoản, hoặc session hết hạn), nên đọc lại ngay sau đó - mọi lời
        # gọi ensure_* tiếp theo nhắm rõ vào đúng tài khoản đó, giờ đã xác nhận còn sống.
        target_account = RedisCache().get(ACTIVE_ACCOUNT_REDIS_KEY) if platform == "facebook" else None
        in_lane = platform == "facebook" and lanes.current_lane() is not None
        if in_lane:
            # Làn Facebook (services/lanes.py): đúng tài khoản của làn, không phải con trỏ active chung.
            target_account = await asyncio.to_thread(lanes.account_for_job)
            if target_account is None:
                raise CrawlJobSkipped("no facebook account free for this lane")
        if platform == "facebook":
            if _stopped():
                logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
                raise CrawlJobSkipped("comments cancelled before start")
            if not await _ensure_facebook_session(
                target_account,
                run_id=run_id,
                platform=platform,
                honor_platform_cancel=not bypass_drain,
                allow_other_account=not in_lane,
            ):
                raise CrawlJobFailed("facebook session refresh failed before comments crawl")
            if target_account is not None and not await asyncio.to_thread(_facebook_session_is_cached, target_account):
                # _ensure_facebook_session đã phải chuyển sang tài khoản khác (tài khoản ghim ban đầu chết) -
                # bỏ ghim để đọc lại tài khoản active mới bên dưới.
                target_account = None
            if target_account is None:
                # Chỉ tin một lần đọc mới ở đây khi ta chưa ghim tài khoản cụ thể nào - next_account() của
                # bootstrap.py vừa chọn một tài khoản (trước đó chưa có gì để ghim), nên đây là chỗ duy
                # nhất biết được lựa chọn đó. Nếu target_account đã được đặt, _ensure_facebook_session đã
                # kiểm tra/bootstrap *đúng tài khoản đó* - đọc lại key dùng chung ở đây có thể lấy phải
                # một thay đổi đồng thời không liên quan và âm thầm gỡ ghim, đúng lỗi mà cả cơ chế này
                # sinh ra để ngăn.
                target_account = RedisCache().get(ACTIVE_ACCOUNT_REDIS_KEY)
        if platform in COMMENTS_PLATFORMS_NEEDING_CACHE:
            if _stopped():
                logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
                raise CrawlJobSkipped("comments cancelled before start")
            comments_ok = (
                await _ensure_comments_cache(
                    platform, post_url, target_account, run_id=run_id, honor_platform_cancel=not bypass_drain
                )
                if platform == "facebook"
                else await _ensure_comments_cache(
                    platform, post_url, run_id=run_id, honor_platform_cancel=not bypass_drain
                )
            )
            if not comments_ok:
                raise CrawlJobFailed("comments cache bootstrap failed")
        include_replies = False
        if platform == "facebook":
            # Cố gắng hết mức, KHÔNG phải cổng chặn phần còn lại của job (2026-09-16, sửa ngay trong
            # ngày đưa vào) - đã xác nhận thực tế việc coi nó là bắt buộc làm lượng comment thật lấy
            # được tụt còn khoảng 0,02% trên một lô 100 bài: phần lớn bài không có chuỗi reply mà
            # bootstrap này dễ bám vào (cần một comment có số reply đủ lớn để phân trang - xem
            # docstring của _ensure_replies_cache), nên bắt buộc nó trước khi chạy BẤT KỲ lượt crawl
            # comment nào đã làm huỷ cả job - mất luôn comment cấp một hoàn toàn lấy được của bài đó -
            # thay vì chỉ mất đúng phần thực sự không có. _fetch_replies của comments.py vốn đã tự hạ
            # cấp êm theo từng comment khi thiếu cache này (log replies_pagination_not_bootstrapped
            # rồi chuyển sang comment kế tiếp) - chưa bao giờ cần chặn cả job vì nó.
            #
            # Bỏ qua hẳn trừ khi request chủ động bật reply: mỗi lần bootstrap reply + lượt gọi reply
            # cho từng comment chiếm phần lớn thời gian trên bài nhiều comment, và đường GraphQL reply
            # hiện trả về edges rỗng với phần lớn tài khoản. Tham số spider mặc định là
            # include_replies=false cũng vì lý do đó.
            include_replies = str(request.get("include_replies", "false")).lower() not in (
                "false",
                "0",
                "no",
                "",
            )
            if include_replies and not await _ensure_replies_cache(
                post_url, target_account, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
            ):
                logger.warning(
                    "replies_cache_unavailable_continuing",
                    post_url=post_url,
                    account=target_account,
                    note="top-level comments will still be fetched; replies on any comment will be skipped this run",
                )

        if _stopped():
            logger.info("comments_crawl_cancelled_before_start", platform=platform, post_id=post_id)
            raise CrawlJobSkipped("comments cancelled before start")

        if platform == "tiktok":
            # tiktok_comments nhận video_id/video_url, không phải post_id/post_url - nó không bao giờ
            # đụng tới doc_id/token đã cache như Facebook/Threads, nên cũng không cần dùng chung tên
            # tham số chung của chúng.
            args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"video_id={post_id}", "-a", f"video_url={post_url}"]
        else:
            args = [SCRAPY_BIN, "crawl", spider_name, "-a", f"post_id={post_id}"]
            if platform == "threads":
                # Không bắt buộc; reply qua REST chỉ cần post_id. Giữ lại để dòng log/debug vẫn có
                # permalink mà dashboard đã xếp hàng.
                args += ["-a", f"post_url={post_url}"]
                # text_feed nhận tới khoảng 100 mỗi trang; consumer trước đây không truyền count và thừa
                # hưởng 25 của spider, đốt ngân sách max_pages sớm trên các chuỗi lớn. Vẫn không vượt được
                # trần hiển thị theo xếp hạng của Threads (cursor kết thúc với phần còn lại của badge chưa
                # đọc).
                args += ["-a", f"count={request.get('count', 100)}"]
            if platform == "facebook":
                # Ghim đúng tài khoản mà mọi lời gọi ensure_* ở trên vừa xác minh (hoặc vừa bootstrap)
                # dùng được cho session+comments+replies, thay vì để FacebookCommentsSpider tự quay về đọc
                # ACTIVE_ACCOUNT_REDIS_KEY khi tiến trình scrapy con thực sự bắt đầu ngay sau đó - đã xảy
                # ra thật (2026-09-16), hai lần: lần đầu là thứ khác (một lần chạy bootstrap đồng thời bên
                # ngoài vòng lặp từng-cái-một của consumer này, ví dụ người tự chạy bootstrap.py bằng tay,
                # hoặc cron refresh_token.sh 4 giờ) trỏ lại ACTIVE_ACCOUNT_REDIS_KEY giữa phép kiểm tra
                # này và lúc tiến trình con đọc nó; lần nữa là việc xoay next_account() của từng lời gọi
                # ensure_* chọn một tài khoản *khác* với lần trước, nên tới cuối thì tài khoản "active"
                # chưa bao giờ thực sự được xác minh đủ cả ba cache cùng lúc. Dùng thẳng target_account
                # (không đọc mới từ Redis) tránh được cả hai.
                if target_account:
                    args += ["-a", f"account={target_account}"]
                # Trang phân trang Comet dày nhất (trình duyệt dùng -1) + bỏ qua đường reply hỏng/chậm trừ
                # khi hàng đợi chủ động bật.
                args += ["-a", f"count={request.get('count', -1)}"]
                args += ["-a", f"include_replies={'true' if include_replies else 'false'}"]
        if request.get("max_pages"):
            args += ["-a", f"max_pages={request['max_pages']}"]

        logger.info("comments_crawl_started", platform=platform, post_id=post_id, run_id=run_id)
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode is None:
        raise CrawlJobSkipped("comments cancelled before start")
    _raise_for_returncode(returncode, f"{platform} comments crawl post_id={post_id}")
    logger.info("comments_crawl_finished", platform=platform, post_id=post_id)
