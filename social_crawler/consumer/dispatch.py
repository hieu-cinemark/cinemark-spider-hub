"""Định tuyến một request crawl tới job tương ứng (kiểm tra huỷ / drain trước)."""

from __future__ import annotations

from typing import Any

from social_crawler.consumer.common import CrawlJobFailed, CrawlJobSkipped, logger
from social_crawler.consumer.jobs.accounts import _check_cookies, _import_cookies, _nurture_accounts, _refresh_token
from social_crawler.consumer.jobs.channel_videos import _run_channel_videos_spider
from social_crawler.consumer.jobs.comments import _run_comments_spider
from social_crawler.consumer.jobs.page_feed import _run_page_feed_spider
from social_crawler.consumer.jobs.post_metrics import _run_post_metrics
from social_crawler.consumer.jobs.profiles import _run_threads_profiles
from social_crawler.consumer.jobs.search import _run_spider
from social_crawler.consumer.process import _cancel_requested, _published_before_stop
from social_crawler.services.task_queue import finish_task, is_platform_draining, start_task


async def _handle_request(request: dict[str, Any]) -> bool:
    """Trả về True khi request bị bỏ qua (Dừng / drain), để consumer lấy message Kafka kế tiếp
    mà không cần khoảng nghỉ giữa các request."""
    platform = request.get("platform") or ""
    kind = request.get("type")
    # bypass_drain đánh dấu message mà cinemark-api gắn là lần kích hoạt có chủ đích riêng của
    # nó (comments/nurture/channel_videos), không phải phần tiếp tục của hàng tồn hàng loạt mà
    # lần Dừng muốn chặn - xem docstring publish_action_request phía cinemark-api. Cũng được
    # truyền xuống cả ba handler đó bên dưới (không chỉ cổng vào này): mỗi handler tự kiểm tra
    # định kỳ lại cùng các tín hiệu drain/huỷ cho cả nền tảng, trước/trong tiến trình con của
    # mình, và phải bỏ qua chúng theo cùng cách, nếu không một tín hiệu cũ từ lần Dừng trước sẽ
    # giết chính job mà bypass_drain muốn cho qua (đã xảy ra thật 2026-09-21 - một lượt nurture
    # bị SIGTERM bởi crawl_job_cancel_platform còn sót chỉ vài giây sau khi bắt đầu).
    bypass_drain = bool(request.get("bypass_drain"))

    # Một lần Dừng chính xác theo từng job (POST /<platform>/jobs/{run_id}/stop của
    # cinemark-api - xem crawl_jobs.cancel_job) nhắm đúng run_id NÀY, được bấm khi nó còn đang
    # xếp hàng chứ chưa chạy. Luôn được tôn trọng, dù có bypass_drain hay không: khác với phép
    # kiểm tra drain cho cả nền tảng bên dưới (mà một lần kích hoạt có chủ đích phải vượt qua
    # được), tín hiệu này chỉ tồn tại vì người dùng đã bấm Dừng đúng job này.
    run_id = request.get("run_id")
    if run_id and _cancel_requested(run_id=run_id):
        logger.info("request_skipped_job_cancel", platform=platform, run_id=run_id, type=kind or "search")
        finish_task(request, "skipped")
        return True

    # Dừng bỏ mọi thứ đã xếp hàng, kể cả các lần kích hoạt có chủ đích - bypass_drain chỉ cho
    # qua những gì được bấm *sau* lần Dừng đó. Không có cái này, các message nurture/comments
    # xếp hàng trước khi Dừng cứ lần lượt bắt đầu trong khi dashboard không hiện gì.
    if bypass_drain and platform and _published_before_stop(request, platform):
        logger.info("request_skipped_stopped", platform=platform, run_id=run_id, type=kind or "search")
        finish_task(request, "skipped")
        return True

    if (
        kind not in ("refresh_token", "cookie_import", "cookie_check")
        and not bypass_drain
        and platform
        and is_platform_draining(platform)
    ):
        logger.info("request_skipped_drain", platform=platform, type=kind or "search", post_id=request.get("post_id"))
        finish_task(request, "skipped")
        return True

    start_task(request)
    try:
        if request.get("type") == "refresh_token":
            await _refresh_token(request)
        elif request.get("type") == "cookie_import":
            await _import_cookies(request)
        elif request.get("type") == "comments":
            await _run_comments_spider(request, bypass_drain=bypass_drain)
        elif request.get("type") == "channel_videos":
            await _run_channel_videos_spider(request, bypass_drain=bypass_drain)
        elif request.get("type") == "page_feed":
            await _run_page_feed_spider(request, bypass_drain=bypass_drain)
        elif request.get("type") == "post_metrics":
            await _run_post_metrics(request, bypass_drain=bypass_drain)
        elif request.get("type") == "profiles":
            await _run_threads_profiles(request, bypass_drain=bypass_drain)
        elif request.get("type") == "nurture":
            await _nurture_accounts(request, bypass_drain=bypass_drain)
        elif request.get("type") == "cookie_check":
            await _check_cookies(request)
        else:
            await _run_spider(request)
    except CrawlJobSkipped as exc:
        # Ghi lý do (28/10/2026): trước đó job chỉ hiện "Đã bỏ qua" trên dashboard, không có cách biết vì sao.
        logger.info(
            "crawl_job_skipped",
            platform=request.get("platform"),
            type=request.get("type") or "search",
            keyword=request.get("keyword"),
            lane=request.get("lane"),
            reason=str(exc),
        )
        finish_task(request, "skipped")
        return True
    except CrawlJobFailed as exc:
        finish_task(request, "failed", error=str(exc))
        return False
    except Exception:
        finish_task(request, "failed")
        raise
    finish_task(request, "done")
    return False


def _request_platform(request: dict[str, Any]) -> str | None:
    """Vòng lặp nền tảng nào nên nhận message này. Chỉ request refresh_token mới nhận mặc định
    "facebook" cũ mà chính _refresh_token đã dùng, cho message xếp hàng từ trước khi hỗ trợ
    nhiều nền tảng - một crawl request thường được kỳ vọng luôn đặt "platform" rõ ràng (xem
    _run_spider, vốn không có mặc định như vậy). Đặt mặc định cho *mọi* loại message ở đây
    trước đây khiến một crawl request không có platform bị vòng lặp facebook nhận rồi lại bị
    từ chối ở đó là unsupported_platform; trả None cho trường hợp đó nghĩa là không vòng lặp
    nào nhận, nên nó rơi xuống dòng log nền tảng không nhận ra bên dưới, đúng như _run_spider
    từng tự tạo ra."""
    if request.get("type") == "refresh_token":
        return request.get("platform", "facebook")
    return request.get("platform")
