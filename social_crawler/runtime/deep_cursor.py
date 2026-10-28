"""Đào bài cũ lùi dần theo từng từ khoá Facebook (27/10/2026) - phần "deep" của hạn mức (services/search_budget.py).

Trước đây bài cũ chỉ đến từ lượt quét 60 ngày mỗi 7 ngày: lần nào cũng lật lại đúng 60 ngày gần nhất (phần lớn đã thấy,
vẫn tốn request) và không bao giờ lùi xa hơn. Giờ mỗi từ khoá có một con trỏ trong Redis: đã đào tới ngày nào. Mỗi job
"deep" đào tiếp WINDOWS_PER_JOB khoảng WINDOW_DAYS ngày lùi về trước rồi lưu con trỏ - không quét lại khoảng đã quét.

- Mốc sàn: ngày công chiếu trừ FLOOR_DAYS (không có ngày chiếu: hôm nay trừ FLOOR_DAYS). Chạm sàn -> từ khoá "đào xong".
- EMPTY_WINDOWS_DONE khoảng liền nhau không có bài nào khớp từ khoá (kể cả bài đã thấy) -> "đào xong" sớm: phía trước
  đó từ khoá chưa tồn tại.
- RESWEEP_DAYS ngày gần nhất được quét lại mỗi RESWEEP_EVERY_DAYS ngày (Facebook index bài muộn; lượt bài mới chỉ lấy
  1-2 trang đầu) - kể cả từ khoá đã đào xong. Lượt quét lại kèm tab Video.

Con trỏ được spider (facebook_search, -a deep_kind=...) cập nhật sau MỖI khoảng quét xong, nên job bị dừng giữa chừng
(hết phần hạn mức / trần giờ) không làm mất phần đã đào. cinemark-api (app/services/crawl_lanes.py) đọc cùng key để
chọn từ khoá còn việc - đổi format ở đây thì sửa cả bên đó.
"""

from __future__ import annotations

import os
import zlib
from datetime import date, timedelta
from typing import Any

from social_crawler.clients.redis import RedisCache
from social_crawler.core.logger import get_logger

logger = get_logger(__name__)

WINDOW_DAYS = int(os.getenv("FACEBOOK_DEEP_WINDOW_DAYS", "7"))
WINDOWS_PER_JOB = int(os.getenv("FACEBOOK_DEEP_WINDOWS_PER_JOB", "2"))
FLOOR_DAYS = int(os.getenv("FACEBOOK_DEEP_FLOOR_DAYS", "365"))
EMPTY_WINDOWS_DONE = int(os.getenv("FACEBOOK_DEEP_EMPTY_WINDOWS_DONE", "4"))
RESWEEP_DAYS = 14
RESWEEP_EVERY_DAYS = 7
_KEY = "fb_deep:{keyword_id}"
_TTL_SECONDS = 180 * 86400


def _load(cache: RedisCache, keyword_id: str) -> dict[str, Any]:
    state = cache.get(_KEY.format(keyword_id=keyword_id))
    return state if isinstance(state, dict) else {}


def _save(cache: RedisCache, keyword_id: str, state: dict[str, Any]) -> None:
    cache.set(_KEY.format(keyword_id=keyword_id), state, ttl_seconds=_TTL_SECONDS)


def floor_for(released_at: str | None, today: date) -> date:
    try:
        anchor = date.fromisoformat(str(released_at or "")[:10])
    except ValueError:
        anchor = today
    return min(anchor, today) - timedelta(days=FLOOR_DAYS)


def resweep_due(state: dict[str, Any], keyword_id: str, today: date) -> bool:
    """Lần quét lại gần nhất đã cũ RESWEEP_EVERY_DAYS ngày; chưa quét lần nào thì chờ tới ngày của từ khoá (lệch theo
    keyword_id) để lượt đầu không dồn cả trăm lượt quét lại vào cùng một ngày."""
    last = state.get("resweep")
    if last:
        return (today - date.fromisoformat(last)).days >= RESWEEP_EVERY_DAYS
    return (today.toordinal() + zlib.crc32(keyword_id.encode())) % RESWEEP_EVERY_DAYS == 0


def next_job(
    keyword_id: str, released_at: str | None, today: date | None = None, cache: RedisCache | None = None
) -> dict[str, Any] | None:
    """Việc kế tiếp của từ khoá: {"kind": "resweep"|"backfill", "start": date, "end": date} hoặc None (hết việc)."""
    cache = cache or RedisCache()
    today = today or date.today()
    state = _load(cache, keyword_id)
    if resweep_due(state, keyword_id, today):
        return {"kind": "resweep", "start": today - timedelta(days=RESWEEP_DAYS - 1), "end": today}
    if state.get("done"):
        return None
    floor = floor_for(released_at, today)
    # Chưa đào lần nào: bắt đầu ngay sau phần RESWEEP_DAYS gần nhất (phần đó do lượt quét lại + lượt bài mới lo).
    oldest = date.fromisoformat(state["oldest"]) if state.get("oldest") else today - timedelta(days=RESWEEP_DAYS - 1)
    end = oldest - timedelta(days=1)
    if end < floor:
        _save(cache, keyword_id, {**state, "done": True, "floor": floor.isoformat()})
        return None
    start = max(floor, end - timedelta(days=WINDOW_DAYS * WINDOWS_PER_JOB - 1))
    return {"kind": "backfill", "start": start, "end": end}


def window_done(
    keyword_id: str, start: date, released_at: str | None, matched: int, cache: RedisCache | None = None
) -> dict[str, Any]:
    """Spider gọi sau mỗi khoảng backfill quét xong (khoảng mới nhất trước): lùi con trỏ về `start`, đếm chuỗi khoảng
    không có bài khớp, đánh dấu đào xong khi chạm sàn / hết bài."""
    cache = cache or RedisCache()
    state = _load(cache, keyword_id)
    floor = floor_for(released_at, date.today())
    oldest = date.fromisoformat(state["oldest"]) if state.get("oldest") else None
    if oldest is None or start < oldest:
        state["oldest"] = start.isoformat()
    state["empty"] = 0 if matched else int(state.get("empty") or 0) + 1
    state["done"] = start <= floor or state["empty"] >= EMPTY_WINDOWS_DONE
    state["floor"] = floor.isoformat()
    _save(cache, keyword_id, state)
    if state["done"]:
        logger.info("deep_keyword_exhausted", keyword_id=keyword_id, oldest=state["oldest"], empty=state["empty"])
    return state


def resweep_done(keyword_id: str, today: date | None = None, cache: RedisCache | None = None) -> None:
    cache = cache or RedisCache()
    state = _load(cache, keyword_id)
    state["resweep"] = (today or date.today()).isoformat()
    _save(cache, keyword_id, state)
