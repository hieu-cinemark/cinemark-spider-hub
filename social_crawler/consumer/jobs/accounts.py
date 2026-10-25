"""Job tài khoản: làm mới token / danh tính, import cookie, nuôi tài khoản, kiểm tra cookie."""

from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.consumer.common import (
    CRAWL_JOB_CANCEL_KEY_TMPL,
    PYTHON_BIN,
    TOKEN_REFRESH_BOOTSTRAP_MODULE,
    TOKEN_REFRESH_QUERY,
    CrawlJobFailed,
    _mark_refresh_result,
    _raise_for_returncode,
    crawl_job_key,
    logger,
)
from social_crawler.consumer.process import _run_subprocess

NURTURE_PARTIAL_EXIT_CODE = 2


async def _refresh_tiktok_identity(request: dict[str, Any]) -> None:
    """Bản tương đương của _refresh_token bên dưới cho TikTok, nhưng có dạng khác: không có
    cache token GraphQL nào để bắt lại. Restore / bước tiếp theo sau import cookie dùng lại
    cùng phần theo dõi job/huỷ, rồi chạy tiktok/auth/bootstrap.py trên một dòng
    platform_accounts đã ghim (device_id/odinId từ cookie đã lưu)."""
    account_id = request.get("account_id")
    account_key = request.get("account_key")
    args = [PYTHON_BIN, "-m", "social_crawler.spiders.tiktok.auth.bootstrap"]
    if account_id is not None:
        args += ["--account-id", str(account_id)]
    elif account_key:
        args += ["--account", str(account_key)]
    else:
        logger.warning("tiktok_refresh_missing_account", request=request)
        raise CrawlJobFailed("tiktok refresh missing account_id/account_key")

    run_id = request.get("run_id")
    cache = RedisCache()
    job_key = crawl_job_key("tiktok")
    if run_id:
        args += ["--run-id", str(run_id)]
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info(
        "token_refresh_started", platform="tiktok", account_id=account_id, account_key=account_key, run_id=run_id
    )
    try:
        returncode = await _run_subprocess(args, run_id=run_id, platform="tiktok", honor_platform_cancel=False)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode != 0:
        logger.error(
            "token_refresh_failed",
            platform="tiktok",
            account_id=account_id,
            account_key=account_key,
            returncode=returncode,
            run_id=run_id,
        )
        _mark_refresh_result(run_id, ok=False)
        _raise_for_returncode(returncode, "tiktok token refresh")
    else:
        logger.info(
            "token_refresh_finished",
            platform="tiktok",
            account_id=account_id,
            account_key=account_key,
            run_id=run_id,
        )
        _mark_refresh_result(run_id, ok=True)


async def _refresh_token(request: dict[str, Any]) -> None:
    """Cùng lệnh mà cron 4 giờ của scripts/refresh_token.sh vẫn chạy cho Facebook - chỉ là kích
    hoạt khi cần thay vì chờ lượt kế tiếp. Cũng xử lý Threads theo cùng cách, vì bootstrap.py
    của nó giống luồng cache token của Facebook từng trường một (xem
    spiders/threads/auth/bootstrap.py). platform/started/finished/failed được log rõ ràng
    (không chỉ suy ra từ tên module) vì refresh_tracker của cinemark-api đọc dần đúng file
    log này và khớp theo "token_refresh_{started,finished,failed} ... platform=<x>" để biết
    khi nào một lần refresh kích hoạt từ dashboard đã xong - xem
    cinemark-api/app/services/refresh_tracker.py."""
    platform = request.get("platform", "facebook")
    if platform == "tiktok":
        await _refresh_tiktok_identity(request)
        return

    module = TOKEN_REFRESH_BOOTSTRAP_MODULE.get(platform)
    if module is None:
        logger.warning("unsupported_refresh_token_platform", platform=platform)
        raise CrawlJobFailed(f"unsupported refresh_token platform {platform}")

    run_id = request.get("run_id")
    # --run-id (khi có) được bind vào context structlog của chính tiến trình con (xem __main__
    # trong auth/bootstrap.py của facebook/threads), nên mọi dòng nó log - không chỉ
    # started/finished/failed của hàm này - đều mang run_id=<x>. refresh_tracker của
    # cinemark-api lọc các dòng đọc được theo đúng dấu đó (xem docstring module đó để biết vì
    # sao khớp chuỗi con platform= trần không đủ chính xác khi crawl_request_consumer.py bắt
    # đầu chạy song song mỗi nền tảng một task thay vì một vòng lặp tuần tự toàn cục).
    args = [PYTHON_BIN, "-m", module, "--query", TOKEN_REFRESH_QUERY]
    account_key = request.get("account_key")
    if account_key:
        # Ghim vào một dòng (restore trên dashboard / bước tiếp theo sau import cookie). Không có
        # cái này, việc xoay vòng bỏ qua tài khoản bị checkpoint và bắt lại token cho tài khoản nào
        # còn khoẻ thay vì tài khoản người vận hành vừa chọn.
        args += ["--account", str(account_key)]
    if run_id:
        args += ["--run-id", str(run_id)]

    cache = RedisCache()
    job_key = crawl_job_key(platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info("token_refresh_started", platform=platform, run_id=run_id)
    try:
        returncode = await _run_subprocess(args, run_id=run_id, platform=platform, honor_platform_cancel=False)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode != 0:
        logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
        _mark_refresh_result(run_id, ok=False)
        _raise_for_returncode(returncode, f"{platform} token refresh")
    else:
        logger.info("token_refresh_finished", platform=platform, run_id=run_id)
        _mark_refresh_result(run_id, ok=True)


async def _import_cookies(request: dict[str, Any]) -> None:
    """Import cookie kích hoạt từ dashboard (xem POST /<platform>/import-cookies của
    cinemark-api) - chạy đúng luồng `bootstrap.py --cookies-file ... --account ...` mà
    người dùng lẽ ra phải tự gõ trong terminal (xem import_cookies trong auth/cookies.py của
    facebook/threads), chỉ là kích hoạt từ form "Nhập cookie" của trang Settings.

    Chỉ tự động hoá bước "đưa cookie đã xuất vào Redis" - bản thân việc đăng nhập/2FA đã xảy
    ra trong một trình duyệt thật do người điều khiển (tài khoản không có cookie thì tự đăng
    nhập qua proxy đã ghim - xem auth/bootstrap.py của facebook/threads).

    Nối thẳng sang một lần refresh token bình thường khi import thành công - --cookies-file
    chỉ gọi import_cookies() (lưu session vào Redis), không bao giờ đi tiếp để bắt các token
    phát lại GraphQL mà một lượt crawl thực sự cần (xem phần điều phối __main__ của
    bootstrap.py). Không có bước thứ hai này, import cookie sẽ để tài khoản trông như đã xong
    trên dashboard nhưng vẫn lỗi ở ngay lượt crawl kế tiếp - cần một lần kích hoạt tay thứ
    hai mà không ai biết để chờ. Cố ý không còn nút/route "refresh login" riêng (xem lịch sử
    token_refresh.py của cinemark-api) - một session mới chỉ cần làm mới khi đã tồn tại, và
    đây là nơi duy nhất tạo ra nó.

    Dùng lại token_refresh_{started,finished,failed} - không phải tên event mới - để
    refresh_tracker.py của cinemark-api và RefreshLogPanel/TokenStatusBadge có sẵn trên
    dashboard hiển thị tiến độ lượt chạy này theo đúng cách đã làm với lần refresh thường,
    không cần nối thêm gì ở frontend."""
    platform = request.get("platform", "facebook")
    account_key = request.get("account_key")
    cookies_json = request.get("cookies")
    run_id = request.get("run_id")
    if not account_key or not cookies_json:
        logger.warning("cookie_import_missing_fields", platform=platform, run_id=run_id)
        raise CrawlJobFailed("cookie import missing account_key or cookies")

    module = TOKEN_REFRESH_BOOTSTRAP_MODULE.get(platform)
    if module is None:
        logger.warning("unsupported_refresh_token_platform", platform=platform, run_id=run_id)
        raise CrawlJobFailed(f"unsupported cookie_import platform {platform}")

    cache = RedisCache()
    job_key = crawl_job_key(platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "refresh_token", "started_at": int(time.time())})

    logger.info("token_refresh_started", platform=platform, run_id=run_id, source="cookie_import")
    try:
        # Bước 1: đưa cookie do người xuất vào Redis. Dùng file tạm, không truyền --cookies-json
        # qua argv: đường --cookies-file của bootstrap.py đã có sẵn và đã được kiểm chứng (đây
        # đúng là thứ người dùng vẫn tự chạy bằng tay hiện nay) - dùng lại nó ở đây nghĩa là không
        # phải sửa CLI/parse gì trong bootstrap.py, chỉ thêm một chỗ gọi mới.
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        try:
            tmp.write(cookies_json)
            tmp.close()
            import_args = [PYTHON_BIN, "-m", module, "--cookies-file", tmp.name, "--account", account_key]
            if run_id:
                import_args += ["--run-id", str(run_id)]
            returncode = await _run_subprocess(
                import_args, run_id=run_id, platform=platform, honor_platform_cancel=False
            )
        finally:
            Path(tmp.name).unlink(missing_ok=True)

        if returncode != 0:
            logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
            _mark_refresh_result(run_id, ok=False)
            _raise_for_returncode(returncode, f"{platform} cookie import")

        # Import kiểu Copy-as-cURL của TikTok đã ghi device_id/odin_id/cookie và đổi account_id
        # thành device_id. Chạy bootstrap lần hai với --account <ghim ban đầu> sẽ không tìm thấy
        # dòng. Danh tính đã đầy đủ.
        if platform == "tiktok":
            logger.info("token_refresh_finished", platform=platform, run_id=run_id, source="cookie_import")
            _mark_refresh_result(run_id, ok=True)
            return

        # Facebook/Threads bắt lại token phát lại GraphQL.
        refresh_args = [PYTHON_BIN, "-m", module, "--query", TOKEN_REFRESH_QUERY, "--account", str(account_key)]
        if run_id:
            refresh_args += ["--run-id", str(run_id)]
        returncode = await _run_subprocess(refresh_args, run_id=run_id, platform=platform, honor_platform_cancel=False)
        if returncode != 0:
            logger.error("token_refresh_failed", platform=platform, returncode=returncode, run_id=run_id)
            _mark_refresh_result(run_id, ok=False)
            _raise_for_returncode(returncode, f"{platform} cookie import token refresh")
        else:
            logger.info("token_refresh_finished", platform=platform, run_id=run_id)
            _mark_refresh_result(run_id, ok=True)
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))


async def _nurture_accounts(request: dict[str, Any], *, bypass_drain: bool = False) -> None:
    """Chạy nurture_accounts.py cho một nền tảng (facebook, threads hoặc tiktok). Settings trên
    dashboard xếp hàng các lượt này dưới dạng type=nurture trên crawl_requests - cùng consumer
    từng-cái-một như crawl, nên một lượt làm ấm không bao giờ chồng lên một lượt tìm kiếm
    của cùng tài khoản/session."""
    platform = request.get("platform")
    if platform not in ("facebook", "threads", "tiktok"):
        logger.warning("nurture_unsupported_platform", platform=platform, request=request)
        raise CrawlJobFailed(f"unsupported nurture platform {platform}")

    args = [PYTHON_BIN, "-m", "social_crawler.nurture_accounts", "--platform", platform]
    account = request.get("account")
    if account:
        # Một tài khoản được chỉ định: vẫn tôn trọng hạn mức hằng ngày, nhưng không chờ vài phút
        # trước khi bắt đầu - người vận hành vừa bấm vào đúng dòng đó.
        args += ["--account", str(account), "--limit", "1", "--gap-min", "30", "--gap-max", "90"]
        args += ["--start-delay-min", "15", "--start-delay-max", "75"]
    else:
        # Làm ấm cả pool: tối đa hai tài khoản mỗi nền tảng, bỏ qua tài khoản đã làm ấm hôm nay,
        # chờ một chút trước lượt lướt đầu tiên và vài phút giữa các tài khoản để một cú bấm không
        # thành một đợt dồn dập.
        # 2 -> 6 tài khoản / lượt (24/10/2026): 10 tài khoản mới cần đủ ngày nuôi trước khi crawl, 2 / lượt thì mất cả tuần.
        args += ["--limit", os.getenv("NURTURE_POOL_LIMIT", "6"), "--gap-min", "120", "--gap-max", "300"]
        args += ["--start-delay-min", "45", "--start-delay-max", "240"]
    args.append("--like" if request.get("like", True) else "--no-like")

    count = request.get("visits", 3)
    try:
        count_n = max(0, min(8, int(count)))
    except TypeError, ValueError:
        count_n = 3
    if platform == "tiktok":
        # Đường tiktok của nurture_accounts.py không có khái niệm --comment (feed của nó là cuộn
        # liên tục, không phải các bài riêng lẻ có ô soạn thảo - xem docstring của module đó) và
        # dùng lại trường "visits" của request mà docstring publish_nurture_request của
        # cinemark-api đã ghi là "số trang hashtag cần vào" cho nền tảng này - --hashtags 0 không
        # hợp lệ ở đó (ít nhất vào 1 trang), khác với --visits 0 của facebook/threads (chỉ
        # comment/lướt, không mở bài nào).
        args += ["--hashtags", str(max(1, count_n))]
    else:
        # Không có trường comment = tắt (xem --comment trong nurture_accounts.py).
        args.append("--comment" if request.get("comment", False) else "--no-comment")
        args += ["--visits", str(count_n)]

    run_id = request.get("run_id")
    if run_id:
        args += ["--run-id", str(run_id)]
    cache = RedisCache()
    job_key = crawl_job_key(platform)
    if run_id:
        cache.set(job_key, {"run_id": run_id, "type": "nurture", "account": account, "started_at": int(time.time())})

    logger.info("nurture_started", platform=platform, account=account, run_id=run_id)
    try:
        returncode = await _run_subprocess(
            args, run_id=run_id, platform=platform, honor_platform_cancel=not bypass_drain
        )
    finally:
        if run_id:
            cache.delete(job_key)
            cache.delete(CRAWL_JOB_CANCEL_KEY_TMPL.format(run_id=run_id))

    if returncode == NURTURE_PARTIAL_EXIT_CODE:
        # nurture_accounts.py trả 2 khi có tài khoản lỗi/bỏ qua (vd. proxy đang nghỉ) - các tài khoản khác vẫn được nuôi,
        # chi tiết đã có trong log từng tài khoản; không báo cả lượt là FAILED.
        logger.info("nurture_finished", platform=platform, account=account, partial=True)
        return
    _raise_for_returncode(returncode, f"{platform} nurture account={account}")
    logger.info("nurture_finished", platform=platform, account=account)


async def _check_cookies(request: dict[str, Any]) -> None:
    """Kiểm tra định kỳ cookie còn đăng nhập không (scripts/check_facebook_cookies.py - không đăng nhập, chỉ mở
    facebook.com bằng cookie sẵn có qua proxy đã ghim). Tài khoản chết được ghi last_check_status='dead' để auto-login
    nạp cookie mới. Bộ lập lịch cinemark-api xếp request này (type=cookie_check) - cùng hàng đợi từng-cái-một với
    crawl nên không bao giờ mở cùng session song song với một lượt crawl."""
    platform = request.get("platform") or "facebook"
    if platform != "facebook":
        raise CrawlJobFailed(f"cookie_check only supports facebook, got {platform}")
    args = [PYTHON_BIN, "-m", "scripts.check_facebook_cookies"]
    stale_hours = request.get("stale_hours")
    if stale_hours:
        args += ["--stale-hours", str(stale_hours)]
    logger.info("cookie_check_started", platform=platform, stale_hours=stale_hours)
    returncode = await _run_subprocess(args, run_id=request.get("run_id"), platform=platform)
    _raise_for_returncode(returncode, f"{platform} cookie check")
    logger.info("cookie_check_finished", platform=platform)
