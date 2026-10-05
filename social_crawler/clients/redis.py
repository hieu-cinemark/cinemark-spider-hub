from __future__ import annotations

import json
import os
import time
from typing import Any

import redis

from social_crawler.logger import get_logger

logger = get_logger(__name__)


class RedisCache:
    """Lớp bọc mỏng quanh redis-py: serialize giá trị thành JSON và thêm prefix cho mọi key để
    key của project này không đụng với key của project khác trên một instance Redis dùng
    chung."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: int | None = None,
        password: str | None = None,
        prefix: str = "social_crawler:",
    ):
        self._prefix = prefix
        self._client = redis.Redis(
            host=host or os.environ.get("REDIS_HOST", "localhost"),
            port=port or int(os.environ.get("REDIS_PORT", "6379")),
            db=db if db is not None else int(os.environ.get("REDIS_DB", "0")),
            password=password or os.environ.get("REDIS_PASSWORD") or None,
            decode_responses=True,
        )

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def get(self, key: str) -> Any:
        raw = self._client.get(self._key(key))
        if raw is None:
            return None
        return json.loads(raw)

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        """Thử lại vài lần khi Redis lỗi tạm thời trước khi bỏ cuộc - không có cái này, một lần
        trục trặc ngắn ngay sau một lần đăng nhập trình duyệt mới (có thể đã cần người giải
        captcha/2FA) sẽ crash với redis.RedisError không được xử lý và âm thầm vứt mất session
        đó, vì nó chỉ được lưu trong Redis, không bao giờ trên đĩa."""
        raw = json.dumps(value, ensure_ascii=False)
        last_exc: redis.RedisError | None = None
        for attempt in range(1, 4):
            try:
                self._client.set(self._key(key), raw, ex=ttl_seconds)
                return
            except redis.RedisError as exc:
                last_exc = exc
                logger.warning("redis_set_failed", key=key, attempt=attempt, error=str(exc))
                if attempt < 3:
                    time.sleep(0.5 * attempt)
        logger.error("redis_set_failed_permanently", key=key, error=str(last_exc))
        raise last_exc

    def _log_op_failed(self, op: str, key: str, exc: redis.RedisError) -> None:
        """Cảnh báo dùng chung cho mọi thao tác ghi bên dưới - khác với set() (có thử lại và đáng
        có event lỗi riêng), đây là các lần ghi bắn-rồi-quên đơn giản hơn, mà lỗ hổng chính là
        trước đây không có *bất kỳ* ngữ cảnh nào được log (op, key) trước khi redis.RedisError
        trần lan lên tới một chỗ except-rồi-chạy-tiếp chung chung nào đó cách vài tầng gọi."""
        logger.warning("redis_op_failed", op=op, key=key, error=str(exc))

    def delete(self, key: str) -> None:
        try:
            self._client.delete(self._key(key))
        except redis.RedisError as exc:
            self._log_op_failed("delete", key, exc)
            raise

    def lrem_by_id(self, key: str, run_id: str) -> None:
        """Bỏ một task JSON đang xếp hàng có id/run_id khớp."""
        full = self._key(key)
        try:
            raw_items = self._client.lrange(full, 0, -1) or []
        except redis.RedisError as exc:
            self._log_op_failed("lrem_by_id", key, exc)
            raise
        for raw in raw_items:
            try:
                item = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if item.get("id") == run_id:
                try:
                    self._client.lrem(full, 1, raw)
                except redis.RedisError as exc:
                    self._log_op_failed("lrem_by_id", key, exc)
                    raise

    def lpush_capped(self, key: str, value: Any, cap: int) -> None:
        full = self._key(key)
        try:
            self._client.lpush(full, json.dumps(value, ensure_ascii=False))
            self._client.ltrim(full, 0, cap - 1)
        except redis.RedisError as exc:
            self._log_op_failed("lpush_capped", key, exc)
            raise

    def incr(self, key: str, amount: int = 1) -> int:
        """Tăng nguyên tử một bộ đếm số nguyên (ví dụ chỉ số xoay vòng) và trả về giá trị mới -
        khác với vòng get()-rồi-set(), cách này an toàn khi có nhiều chỗ gọi cùng lúc (ví dụ
        cron và lượt chạy tay chồng lên nhau) vì INCRBY của Redis là một thao tác nguyên tử duy
        nhất."""
        try:
            return self._client.incrby(self._key(key), amount)
        except redis.RedisError as exc:
            self._log_op_failed("incr", key, exc)
            raise

    def expire(self, key: str, ttl_seconds: int) -> None:
        """Đặt/làm mới TTL của một key mà không đụng tới giá trị - dùng sau incr() để bật cửa sổ
        trượt ở lần tăng đầu tiên của một bộ đếm mới, vì riêng incr() không bao giờ đặt hạn
        (một bộ đếm không ai đụng tới sẽ sống mãi)."""
        try:
            self._client.expire(self._key(key), ttl_seconds)
        except redis.RedisError as exc:
            self._log_op_failed("expire", key, exc)
            raise

    def exists(self, key: str) -> bool:
        return bool(self._client.exists(self._key(key)))

    def sadd(self, key: str, *members: str) -> int:
        """Thêm phần tử vào một set (ví dụ các id đã crawl) - trả về số phần tử mới được thêm."""
        if not members:
            return 0
        try:
            return self._client.sadd(self._key(key), *members)
        except redis.RedisError as exc:
            self._log_op_failed("sadd", key, exc)
            raise

    def srem(self, key: str, *members: str) -> int:
        """Bỏ phần tử khỏi một set - dùng để hoàn tác sadd() khi item chưa publish được."""
        if not members:
            return 0
        try:
            return self._client.srem(self._key(key), *members)
        except redis.RedisError as exc:
            self._log_op_failed("srem", key, exc)
            raise

    def sismember(self, key: str, member: str) -> bool:
        try:
            return bool(self._client.sismember(self._key(key), member))
        except redis.RedisError as exc:
            self._log_op_failed("sismember", key, exc)
            raise

    def add_if_new(self, key: str, ttl_seconds: int) -> bool:
        """Đánh dấu nguyên tử `key` là đã thấy trong ttl_seconds - True ở lần đầu (key chưa tồn
        tại), False nếu vẫn còn trong cửa sổ TTL của lần gọi trước. Cùng hợp đồng "cái này có
        mới không" như sadd(), nhưng TTL theo từng key thay vì một TTL của cả set phủ mọi phần
        tử như nhau - cho chỗ gọi coi *cùng* một id là mới lại sau khoảng ttl_seconds, thay vì
        nhớ nó mãi mãi (xem comment của SEEN_POSTS_TTL_SECONDS để biết vì sao nhớ vĩnh viễn là
        sai với nội dung có số liệu cứ thay đổi)."""
        try:
            return bool(self._client.set(self._key(key), "1", nx=True, ex=ttl_seconds))
        except redis.RedisError as exc:
            self._log_op_failed("add_if_new", key, exc)
            raise

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except redis.RedisError as exc:
            logger.error("redis_connection_failed", error=str(exc))
            return False


def enable_dedupe_cache(spider_logger: Any) -> RedisCache | None:
    """Dùng chung cho start() của mọi spider: thử kết nối Redis và trả về một RedisCache để
    khử trùng nếu kết nối được, hoặc None để âm thầm quay về chỉ khử trùng trong lượt chạy -
    chỗ gọi chỉ việc `self._cache = enable_dedupe_cache(logger)` khi tham số `dedupe` của
    chúng được bật."""
    cache = RedisCache()
    if cache.ping():
        spider_logger.info("cross_run_dedupe_enabled")
        return cache
    spider_logger.warning("cross_run_dedupe_disabled", reason="redis_not_reachable")
    return None
