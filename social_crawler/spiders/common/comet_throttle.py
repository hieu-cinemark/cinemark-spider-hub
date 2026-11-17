"""Bóp nhịp request của client GraphQL Comet (mixin).

Tách từ comet_graphql_client.py (17/11/2026).
"""

from __future__ import annotations

import random
import time

import redis

from social_crawler.core.logger import get_logger

logger = get_logger(__name__)

from social_crawler.spiders.common.comet_errors import NetworkError, RequestBudgetExceeded

# Điều chỉnh kiểu AIMD cho khoảng bóp nhịp thích ứng theo tài khoản (xem
# CometGraphQLClient._adjust_interval): tăng nhanh khi có bất kỳ dấu hiệu căng thẳng nào
# (một lần thử lại là đủ để phản ứng), giảm chậm để một request sạch ngay sau một đợt khó
# khăn không xoá ngay sự thận trọng.
_ADAPTIVE_INTERVAL_GROWTH_FACTOR = 1.7

_ADAPTIVE_INTERVAL_DECAY_FACTOR = 0.85

# Một khoảng đã tăng sống được bao lâu mà không có tín hiệu căng thẳng mới trước khi
# _current_interval quay về đọc lại MIN_REQUEST_INTERVAL_SECONDS - một tài khoản đã khó
# khăn 10 phút từ một giờ trước thì giờ không nên còn bị bóp nhịp quá thận trọng.
_ADAPTIVE_INTERVAL_TTL_SECONDS = 1800


class ThrottleMixin:
    """Bóp nhịp request của client: khoảng giãn cách thích ứng (AIMD), nghỉ theo tài khoản, đếm request và hạn mức. Trộn vào CometGraphQLClient."""

    def _throttle_key(self) -> str:
        return self.THROTTLE_REDIS_KEY_TMPL.format(account=self._account)

    def _current_interval(self) -> float:
        """Khoảng giãn cách cơ sở dùng ngay lúc này - bình thường là MIN_REQUEST_INTERVAL_SECONDS,
        hoặc một giá trị cao hơn lưu trong Redis nếu tài khoản này gần đây gặp lỗi
        429/5xx/mạng (xem _adjust_interval). Lưu bền (không chỉ trong bộ nhớ) vì các lần chạy
        bootstrap.py/scrapy crawl là tiến trình con sống ngắn - không có Redis thì một lượt chạy
        bị bóp ngay trước khi thoát sẽ không dạy được gì cho lượt sau."""
        stored = self._redis.get(self._throttle_key())
        if stored is None:
            return self.MIN_REQUEST_INTERVAL_SECONDS
        return max(self.MIN_REQUEST_INTERVAL_SECONDS, float(stored))

    def _adjust_interval(self, *, stressed: bool) -> None:
        """Được gọi sau khi mỗi request kết thúc: tăng khoảng đã lưu khi có bất kỳ tín hiệu
        429/5xx/mạng nào, giảm dần lại khi response sạch và lần gọi này chưa có tín hiệu nào. Xem
        các hằng _ADAPTIVE_INTERVAL_* cấp module cho hệ số tăng/giảm và TTL."""
        key = self._throttle_key()
        stored = self._redis.get(key)
        if stored is None:
            if not stressed:
                return  # đã ở mức cơ sở - không có gì để lưu
            current = self.MIN_REQUEST_INTERVAL_SECONDS
        else:
            current = max(self.MIN_REQUEST_INTERVAL_SECONDS, float(stored))

        if stressed:
            new_interval = min(self.ADAPTIVE_INTERVAL_MAX_SECONDS, current * _ADAPTIVE_INTERVAL_GROWTH_FACTOR)
        else:
            new_interval = max(self.MIN_REQUEST_INTERVAL_SECONDS, current * _ADAPTIVE_INTERVAL_DECAY_FACTOR)

        if new_interval <= self.MIN_REQUEST_INTERVAL_SECONDS:
            self._redis.delete(key)
            return

        logger.info(
            "adaptive_interval_adjusted",
            platform=self.PLATFORM,
            account=self._account,
            stressed=stressed,
            interval_seconds=round(new_interval, 2),
        )
        self._redis.set(key, new_interval, ttl_seconds=_ADAPTIVE_INTERVAL_TTL_SECONDS)

    def _throttle(self) -> None:
        """Giãn cách các request tới nền tảng - không có gì khác làm việc này, vì mọi spider ở đây
        gọi thẳng curl_cffi thay vì đi qua downloader của Scrapy. Mốc request trước lưu cả trong Redis theo tài khoản
        (24/10/2026): mỗi job là một tiến trình con mới, chỉ nhớ trong bộ nhớ thì request đầu của job nào cũng bắn ngay."""
        # Mọi lần gửi thật đều đi qua đây, nên đây là chỗ duy nhất cần kiểm tra quyền dùng tài khoản.
        self._ensure_lease()
        self._count_request()
        base_interval = self._current_interval()
        if self.PLATFORM == "facebook":
            self._throttle_facebook(base_interval)
            return
        last_key = f"{self.PLATFORM}:last_request_at:{self._account}"
        try:
            shared_last = float(self._redis.get(last_key) or 0) or None
        except (TypeError, ValueError, redis.RedisError) as exc:
            if self.PLATFORM == "facebook":
                raise NetworkError("Facebook throttle state unavailable; no request sent") from exc
            shared_last = None
        if shared_last and (self._last_request_at is None or shared_last > self._last_request_at):
            self._last_request_at = shared_last
        if self._last_request_at is not None:
            target_gap = base_interval + random.uniform(0, self.REQUEST_INTERVAL_JITTER_SECONDS)
            if self.LONG_PAUSE_CHANCE > 0 and random.random() < self.LONG_PAUSE_CHANCE:
                target_gap += random.uniform(self.LONG_PAUSE_MIN_SECONDS, self.LONG_PAUSE_MAX_SECONDS)
            remaining = target_gap - (time.time() - self._last_request_at)
            if remaining > 0:
                logger.debug(
                    "throttling", delay_seconds=round(remaining, 2), base_interval_seconds=round(base_interval, 2)
                )
                time.sleep(remaining)
                self._ensure_lease()  # lease có thể mất trong lúc chờ
        self._last_request_at = time.time()
        try:
            self._redis.set(last_key, self._last_request_at, ttl_seconds=3600)
        except redis.RedisError as exc:
            if self.PLATFORM == "facebook":
                raise NetworkError("Facebook throttle state could not be saved; no request sent") from exc

    def _mark_request_finished(self) -> None:
        """Chia sẻ mốc xong response với job sau, kể cả khi lỗi mạng."""
        self._last_response_at = time.time()
        new_removals = self._removed_cookies() - getattr(self, "_forgotten_cookies", set())
        if new_removals:
            from social_crawler.spiders.facebook.auth.http_refresh import forget_removed_cookies

            self._forgotten_cookies = getattr(self, "_forgotten_cookies", set()) | new_removals
            forget_removed_cookies(self, new_removals)
        try:
            self._redis.set(f"facebook:last_response_at:{self._account}", self._last_response_at, ttl_seconds=86400)
        except redis.RedisError as exc:
            raise NetworkError("Facebook response pacing state unavailable; requests stopped") from exc

    def _throttle_asset(self) -> None:
        """Bundle CDN không cookie vẫn cần lease và tính vào ngân sách, nhưng không tính vào thời gian nghỉ của tài khoản:
        trình duyệt tải bundle theo đợt, và mỗi cái 5-10 giây thì khoảng mười cái là hết hạn dò."""
        self._ensure_lease()
        self._count_request()
        time.sleep(random.uniform(*self.ASSET_GAP_SECONDS))

    def _throttle_facebook(self, base_interval: float) -> None:
        from social_crawler.runtime.facebook_pacing import rest_range

        cookies = (getattr(self, "_cache", None) or {}).get("cookies") or {}
        identity = str(cookies.get("c_user") or self._account)
        minimum, maximum = rest_range(identity, self.MIN_REQUEST_INTERVAL_SECONDS, self.REQUEST_INTERVAL_JITTER_SECONDS)
        scale = max(1, base_interval / self.MIN_REQUEST_INTERVAL_SECONDS)
        minimum, maximum = minimum * scale, maximum * scale
        if not getattr(self, "_rest_profile_logged", False):
            logger.info(
                "facebook_request_rest_profile",
                account=self._account,
                min_seconds=round(minimum, 2),
                max_seconds=round(maximum, 2),
            )
            self._rest_profile_logged = True
        try:
            # Mốc bắt đầu chỉ là dự phòng khi chuyển đổi/sập mà chưa ghi được response nào.
            last = float(
                self._redis.get(f"facebook:last_response_at:{self._account}")
                or self._redis.get(f"facebook:last_request_at:{self._account}")
                or getattr(self, "_last_response_at", None)
                or getattr(self, "_last_request_at", None)
                or 0
            )
            if last:
                target = random.uniform(minimum, maximum)
                if self.LONG_PAUSE_CHANCE > 0 and random.random() < self.LONG_PAUSE_CHANCE:
                    target += random.uniform(self.LONG_PAUSE_MIN_SECONDS, self.LONG_PAUSE_MAX_SECONDS)
                # Bỏ qua thời gian âm do đồng hồ lùi: tối đa một lần nghỉ sau khi đồng hồ được chỉnh.
                remaining = max(0, target - max(0, time.time() - last))
                if remaining > 0:
                    logger.info("facebook_request_wait", account=self._account, delay_seconds=round(remaining, 2))
                    time.sleep(remaining)
            self._ensure_lease()
            self._last_request_at = time.time()
            self._redis.set(f"facebook:last_request_at:{self._account}", self._last_request_at, ttl_seconds=86400)
        except (TypeError, ValueError, redis.RedisError) as exc:
            raise NetworkError("Facebook request pacing unavailable; no request sent") from exc

    def _count_request(self) -> None:
        """Hạn mức request mỗi tài khoản (services/search_budget.py): Facebook theo ngày + theo phần (budget_share) +
        theo giờ; Threads theo giờ."""
        from social_crawler.runtime import search_budget

        if self.PLATFORM == "threads":
            if search_budget.THREADS_HOURLY_CAP <= 0:
                return
            try:
                hour = search_budget.count_hour_request("threads", self._account, self._redis)
            except redis.RedisError:
                return
            if self.enforce_hourly_cap and hour > search_budget.hourly_hard_cap("threads"):
                raise RequestBudgetExceeded(f"account {self._account!r} used {hour} Threads requests this hour")
            return
        if self.PLATFORM != "facebook":
            return
        share = self.budget_share
        try:
            from social_crawler.runtime import facebook_run_budget

            facebook_run_budget.reserve(self._account, self._redis, self._ensure_lease, self._verify_proxy_after_rest)
            used = search_budget.count_request(self._account, self._redis, share)
            hour = search_budget.used_this_hour("facebook", self._account, self._redis)
            factor = search_budget.ramp_factor(self._account, self._redis)
            if search_budget.DAILY_CAP > 0:
                cap = search_budget.hard_cap() * factor
                if used > cap:
                    raise RequestBudgetExceeded(f"account {self._account!r} used {used} Facebook requests today")
                if (
                    share
                    and search_budget.used_share(self._account, share, self._redis)
                    > search_budget.share_hard_cap(share) * factor
                ):
                    raise RequestBudgetExceeded(f"account {self._account!r} exceeded its {share} budget")
            else:
                cap = search_budget.new_account_cap(self._account, self._redis)
                if cap and used > cap * search_budget.HARD_CAP_FACTOR:
                    raise RequestBudgetExceeded(f"new account {self._account!r} exceeded its daily budget")
            if search_budget.HOURLY_CAP > 0 and hour > search_budget.hourly_hard_cap("facebook") * factor:
                raise RequestBudgetExceeded(f"account {self._account!r} used {hour} Facebook requests this hour")
        except redis.RedisError as exc:
            raise NetworkError("Facebook budget unavailable; no request sent") from exc

    def _verify_proxy_after_rest(self) -> None:
        """Proxy xoay (13/11/2026): IP đổi giữa các lần nghỉ là bình thường - không còn đo lại IP thoát sau khi nghỉ."""
