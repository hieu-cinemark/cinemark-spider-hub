"""Số like/comment/share của một bài Facebook đã biết link, KHÔNG cần đăng nhập (2026-10-25).

Trang bài công khai (permalink bài page, bài group công khai, /reel/<id>) trả HTML có sẵn khối feedback của bài khi
request trông như Chrome mở link từ thanh địa chỉ - thiếu header sec-fetch-* là Facebook trả 400 "Sorry, something went
wrong" cho mọi link. Test tay 2026-10-25 (8 bài, IP nhà): 8/8 khớp số trong D1. Reel có like + comment (không có lượt
xem, không có share); bài page/group có reaction + comment + share. Embed plugin (/plugins/post.php) trả 200 nhưng
không có số nên không dùng.

Không đụng tài khoản nào - chỉ cần một IP chưa bị Facebook chặn. Dùng bởi job type="post_metrics"
(consumer/jobs/post_metrics.py) và scripts/probe_fb_logged_out.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from curl_cffi import requests as cffi_requests

from social_crawler.fingerprint import CURL_CFFI_IMPERSONATE_TARGET

REQUEST_TIMEOUT_SECONDS = 30

# Đúng bộ header Chrome gửi khi mở link từ thanh địa chỉ. curl_cffi impersonate lo phần TLS/HTTP2 giống Chrome.
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
}

# Trang reel/video kèm hàng chục reel gợi ý, mỗi cái một khối số liệu - chỉ tìm trong đoạn ngay sau id của bài.
_VIDEO_WINDOW_CHARS = 20_000

# Trạng thái coi là IP bị chặn (đếm vào cooldown proxy). "unavailable" (bài xoá / riêng tư) không tính.
BLOCKED_STATUSES = {"login_redirect", "no_metrics", "timeout", "http_error"}


@dataclass
class FetchResult:
    status: str  # ok | unavailable | login_redirect | no_metrics | timeout | http_error
    http_status: int
    final_url: str
    html: str
    metrics: dict[str, int | None]


def video_id(url: str) -> str | None:
    match = re.search(r"/(?:reel|videos)/(\d+)", url)
    return match.group(1) if match else None


def parse_metrics(html: str, url: str) -> dict[str, int | None]:
    """{"like", "comment", "share"} của ĐÚNG bài, {} khi trang không có khối số liệu. share = None khi trang không có
    (reel). Bài page/group: khối feedback đầu tiên là của bài (khớp 4/4 lúc test tay)."""
    vid = video_id(url)
    if vid:
        for match in re.finditer(vid, html):
            window = html[match.start() : match.start() + _VIDEO_WINDOW_CHARS]
            like = re.search(r'"unified_reactors":\{"count":(\d+)', window)
            if like:
                comment = re.search(r'"total_comment_count":(\d+)', window)
                return {
                    "like": int(like.group(1)),
                    "comment": int(comment.group(1)) if comment else None,
                    "share": None,
                }
    like = re.search(r'"reaction_count":\{"count":(\d+)\}', html)
    if not like:
        return {}
    comment = re.search(r'"comment_rendering_instance":\{"comments":\{"total_count":(\d+)', html)
    share = re.search(r'"share_count":\{"count":(\d+)', html)
    return {
        "like": int(like.group(1)),
        "comment": int(comment.group(1)) if comment else None,
        "share": int(share.group(1)) if share else None,
    }


def classify(http_status: int, final_url: str, html: str, metrics: dict) -> str:
    if "/login" in final_url or "/checkpoint" in final_url:
        return "login_redirect"
    if http_status == 404:
        return "unavailable"
    if http_status != 200:
        return "http_error"
    if metrics:
        return "ok"
    lowered = html.lower()
    if "content isn't available" in lowered or "nội dung này hiện không" in lowered:
        return "unavailable"
    return "no_metrics"


def fetch_post_metrics(url: str, proxy: str | None) -> FetchResult:
    """Một request GET (đồng bộ - chỗ gọi async bọc bằng asyncio.to_thread). Không giữ cookie giữa các lần gọi."""
    proxies = {"http": proxy, "https": proxy} if proxy else None
    try:
        response = cffi_requests.get(
            url,
            headers=HEADERS,
            impersonate=CURL_CFFI_IMPERSONATE_TARGET,
            proxies=proxies,
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=True,
        )
    except cffi_requests.RequestsError:
        return FetchResult("timeout", 0, url, "", {})
    html = response.text or ""
    final_url = str(response.url or url)
    metrics = parse_metrics(html, url) if response.status_code == 200 else {}
    return FetchResult(
        classify(response.status_code, final_url, html, metrics), response.status_code, final_url, html, metrics
    )
