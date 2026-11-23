"""Thăm dò quá chỗ TikTok báo hết feed hashtag (17/11/2026).

`/api/challenge/item_list/` trả `hasMore=false` ở một cursor, nhưng gửi cursor LỚN HƠN thì vẫn ra video. Nên "hết" của
TikTok là hết của MỘT cursor, không phải hết dữ liệu: khi gặp chỗ hết, thử cộng lần lượt `PROBE_CURSOR_STEPS` vào cursor
vừa gửi; trang đầu tiên có video được spider dùng tiếp như một trang bình thường - vẫn khử trùng theo video_id, vẫn dừng
theo MAX_CONSECUTIVE_EMPTY_NEW_PAGES nếu chỉ toàn bài đã có.

Mốc nhảy là cursor ĐÃ GỬI, không phải cursor trong response. Đo thật trên #traibuonnguoibymoli (17/11/2026): feed hết ở
cursor 90 nhưng response ghi cursor 110; đi tiếp từ 110 chỉ ra 1 video mới sau 6 request, còn đi từ 90 ra 11 video mới sau
9 request (91 -> 2, 99 -> 6, 100 -> 3). Cùng một cursor cũng không cho cùng một kết quả giữa hai lượt chạy (lượt thì trang
cuối ra 19 video, lượt thì 0), nên vùng rìa là thứ phải thăm dò chứ không suy ra được.

Ngân sách `tiktok_probe_requests` (proxy_settings, 0 = tắt) tính cho CẢ lượt chạy: mỗi request thừa là một lần rủi ro IP
bị phạt, nên vùng rìa không được đốt vô hạn. Body rỗng lúc thăm dò là kết quả bình thường: gọi với count_block=False để
nó không tính vào chuỗi 3 lần rỗng vốn tự tắt tài khoản TikTok (client.py).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from social_crawler.constants.tiktok import PROBE_CURSOR_STEPS
from social_crawler.core.logger import get_logger
from social_crawler.spiders.tiktok.client import TikTokBlockedError
from social_crawler.spiders.tiktok.features.hashtag_search.extract import extract_response

logger = get_logger(__name__)

Fetch = Callable[[int], Awaitable[dict[str, Any]]]


class ProbeBudget:
    """Số request thăm dò còn lại của cả lượt chạy."""

    def __init__(self, total: int) -> None:
        self.left = max(0, int(total))

    def take(self) -> bool:
        if self.left <= 0:
            return False
        self.left -= 1
        return True


async def probe_past_end(
    fetch: Fetch,
    base_cursor: int,
    budget: ProbeBudget,
    *,
    hashtag: str = "",
    has_new: Callable[[list[dict[str, Any]]], bool] | None = None,
) -> tuple[int, dict[str, Any]] | None:
    """(cursor, response) của trang đầu tiên sau `base_cursor` có bài CHƯA THẤY, hoặc None khi hết ngân sách / hết thật.

    `fetch(cursor)` phải gọi item_list với count_block=False. Trả luôn response để chỗ gọi khỏi tốn thêm một request.

    `has_new`: trang chỉ tính là "còn dữ liệu" khi có bài chưa thấy. Vùng rìa trả rất nhiều bài trùng - đo
    #traigiamhanhphucbymoli 17/11: nhận "có video" là đủ thì nó đi 61, 62, ... 68 hết 8 request mà chỉ ra 2 bài mới.
    """
    for step in PROBE_CURSOR_STEPS:
        if not budget.take():
            logger.info("tiktok_probe_budget_spent", hashtag=hashtag, base_cursor=base_cursor)
            return None
        cursor = base_cursor + step
        try:
            response = await fetch(cursor)
        except TikTokBlockedError as exc:
            # Rỗng ở vùng rìa là chuyện thường, không phải danh tính hỏng: thử bước nhảy kế.
            logger.info("tiktok_probe_empty_body", hashtag=hashtag, cursor=cursor, error=exc)
            continue
        videos = extract_response(response)
        fresh = bool(videos) and (has_new is None or has_new(videos))
        logger.info(
            "tiktok_probe_page",
            hashtag=hashtag,
            cursor=cursor,
            fetched=len(videos),
            has_new=fresh,
            budget_left=budget.left,
        )
        if fresh:
            return cursor, response
    return None
