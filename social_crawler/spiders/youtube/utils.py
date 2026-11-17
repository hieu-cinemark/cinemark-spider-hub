import re

from social_crawler.spiders.youtube._compat import Logger

logger = Logger.get(__name__)


# "1,7 N" (nghìn), "2,3 Tr" (triệu), "1 tỷ"; tiếng Anh "1.7K", "2.3M", "1B". Bản của data-miner đọc "1,7 N" thành 17 và
# "426.197 lượt xem" đúng nhưng "2,042" thành 2 - viết lại 13/11/2026.
_SUFFIX = {
    "n": 1_000,
    "nghìn": 1_000,
    "k": 1_000,
    "tr": 1_000_000,
    "triệu": 1_000_000,
    "m": 1_000_000,
    "tỷ": 1_000_000_000,
    "b": 1_000_000_000,
}
_NUM_RE = re.compile(r"(\d[\d.,]*)\s*(nghìn|triệu|tỷ|tr|n|k|m|b)?(?![\w])", re.IGNORECASE)


def parse_view_count(text) -> int:
    """ "426.197 lượt xem" -> 426197, "1,7 N lượt xem" -> 1700, "1,2 Tr" -> 1200000, "2,042" -> 2042, "1.7K" -> 1700."""
    if isinstance(text, (int, float)):
        return int(text)
    raw = str(text or "").replace("\xa0", " ").strip()
    match = _NUM_RE.search(raw)
    if not match:
        return 0
    number, suffix = match.group(1), (match.group(2) or "").lower()
    if suffix:
        try:
            return int(float(number.replace(",", ".")) * _SUFFIX[suffix])
        except ValueError:
            return 0
    digits = re.sub(r"[.,]", "", number)
    return int(digits) if digits.isdigit() else 0


_REL_UNITS = (
    (("giây", "second"), 1),
    (("phút", "minute"), 60),
    (("giờ", "hour"), 3600),
    (("ngày", "day"), 86400),
    (("tuần", "week"), 7 * 86400),
    (("tháng", "month"), 30 * 86400),
    (("năm", "year"), 365 * 86400),
)


def parse_relative_time(text: str | None, now: float | None = None) -> int | None:
    """ "17 giờ trước" / "2 days ago" / "1 tháng trước (đã chỉnh sửa)" -> epoch ước lượng, None khi không đọc được. Kết quả
    tìm kiếm và bình luận chỉ có dạng tương đối; ngày đăng chính xác của video lấy từ API player (detail.get_video_meta)."""
    import time

    match = re.search(r"(\d+)\s*([^\d\s]+)", (text or "").lower())
    if not match:
        return None
    amount, unit = int(match.group(1)), match.group(2)
    for names, seconds in _REL_UNITS:
        if any(unit.startswith(name) for name in names):
            return int((now or time.time()) - amount * seconds)
    return None
