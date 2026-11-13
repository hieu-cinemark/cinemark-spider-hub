import asyncio
import re
from functools import wraps

import httpx

from social_crawler.spiders.youtube._compat import Logger
from social_crawler.spiders.youtube._compat import registry
from social_crawler.spiders.youtube._compat import YouTubeStructureChangedError

logger = Logger.get(__name__)


# "1,7 N" (nghìn), "2,3 Tr" (triệu), "1 tỷ"; tiếng Anh "1.7K", "2.3M", "1B". Bản của data-miner đọc "1,7 N" thành 17 và
# "426.197 lượt xem" đúng nhưng "2,042" thành 2 - viết lại 13/11/2026.
_SUFFIX = {"n": 1_000, "nghìn": 1_000, "k": 1_000, "tr": 1_000_000, "triệu": 1_000_000, "m": 1_000_000,
           "tỷ": 1_000_000_000, "b": 1_000_000_000}
_NUM_RE = re.compile(r"(\d[\d.,]*)\s*(nghìn|triệu|tỷ|tr|n|k|m|b)?(?![\w])", re.IGNORECASE)


def parse_view_count(text) -> int:
    """"426.197 lượt xem" -> 426197, "1,7 N lượt xem" -> 1700, "1,2 Tr" -> 1200000, "2,042" -> 2042, "1.7K" -> 1700."""
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


def retry_on_failure(max_retries=3, delay=1):
    """Retry decorator with linear backoff. Raises immediately on YouTubeStructureChangedError."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except YouTubeStructureChangedError as e:
                    logger.critical(
                        f"YouTube structure changed in {func.__name__}: {e}",
                        extra={"extra_data": {"context": e.context}},
                    )
                    raise
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if isinstance(e, httpx.HTTPStatusError) and 400 <= e.response.status_code < 500:
                        raise
                    last_exception = e
                    if isinstance(
                        e,
                        (
                            httpx.ProxyError,
                            httpx.ConnectError,
                            httpx.RemoteProtocolError,
                        ),
                    ):
                        registry.rotate()
                    if attempt < max_retries - 1:
                        wait_time = delay * (attempt + 1)
                        logger.warning(
                            "Attempt %d/%d for %s failed, retrying in %ds: %s",
                            attempt + 1,
                            max_retries,
                            func.__name__,
                            wait_time,
                            e,
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error(
                        " All %d retries exhausted for %s",
                        max_retries,
                        func.__name__,
                        exc_info=True,
                    )
                    raise last_exception

        return wrapper

    return decorator


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
    """"17 giờ trước" / "2 days ago" / "1 tháng trước (đã chỉnh sửa)" -> epoch ước lượng, None khi không đọc được. Kết quả
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
