"""
Worker hồ sơ kênh Threads (follower) cho trang "Xuất thông tin theo kênh" của cinemark - từ 20/10/2026.

cinemark-api (app/services/channel_profiles.py) chọn kênh cần hỏi, SADD handle vào PROFILE_QUEUE_REDIS_KEY rồi xếp
type=profiles lên crawl_requests; crawl_request_consumer.py chạy module này trong vòng lặp từng-cái-một của Threads (không
bao giờ chồng lên một lượt search/comment trên cùng session). Mỗi handle: SPOP -> fetch_profile (query hovercard, cùng
session search đã bootstrap) -> HSET kết quả JSON vào PROFILE_RESULTS_REDIS_KEY. cinemark-api gom hash ở vòng scheduler kế
tiếp và ghi D1 channel_profiles.

Lỗi session/checkpoint/rate limit/mạng/response lạ: trả handle đang hỏi về hàng đợi rồi dừng cả lượt (tài khoản Threads
hiếm - không cố đốt thêm), thoát mã khác 0 để consumer ghi failed (lỗi proxy: PROXY_EXHAUSTED_EXIT_CODE để consumer xếp
lại request). Handle còn lại vẫn nằm trong hàng đợi cho lượt sau.

Chạy:
    python -m social_crawler.spiders.threads.features.profiles.profiles --max 200
    python -m social_crawler.spiders.threads.features.profiles.profiles --username zuck   # hỏi 1 handle, in ra, không đụng hàng đợi
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import UTC, datetime

from social_crawler.clients.redis import RedisCache
from social_crawler.constants.threads import (
    PROFILE_GAP_MAX_SECONDS,
    PROFILE_GAP_MIN_SECONDS,
    PROFILE_MAX_PER_RUN,
    PROFILE_QUEUE_REDIS_KEY,
    PROFILE_RESULTS_REDIS_KEY,
    PROFILE_RESULTS_TTL_SECONDS,
)
from social_crawler.constants.tiktok import PROXY_EXHAUSTED_EXIT_CODE
from social_crawler.core.logger import bind_run_id, get_logger
from social_crawler.runtime import pool
from social_crawler.spiders.error_alerts import note_transient_error
from social_crawler.spiders.threads.auth.graphql_client import (
    CheckpointRequiredError,
    NetworkError,
    RateLimitedError,
    SessionExpiredError,
    ThreadsGraphQLClient,
)
from social_crawler.spiders.threads.features.profiles.extract import ProfileQueryError

logger = get_logger(__name__)

_NETWORK_HINT = (
    "check connectivity to the platform_proxies row for platform='threads' - "
    "this is a proxy/network problem, not a dead session, re-running bootstrap.py won't help."
)


def _network_exit_code(exc: NetworkError) -> int:
    # Cùng quy ước với spider TikTok: nguyên nhân là cạn/sập proxy -> consumer xếp lại request và nghỉ nền tảng.
    return PROXY_EXHAUSTED_EXIT_CODE if isinstance(exc.__cause__, pool.ProxyPoolExhaustedError) else 1


def _result(handle: str, profile: dict | None) -> dict:
    return {
        "handle": handle,
        "found": profile is not None,
        **(profile or {}),
        "fetched_at": datetime.now(tz=UTC).isoformat(),
    }


def run(max_profiles: int = PROFILE_MAX_PER_RUN) -> int:
    """Một lượt: tối đa `max_profiles` handle từ hàng đợi. Trả về mã thoát."""
    cache = RedisCache()
    try:
        client = ThreadsGraphQLClient(redis_cache=cache)
    except SessionExpiredError as exc:
        logger.error(
            "session_expired",
            error=str(exc),
            hint='python -m social_crawler.spiders.threads.auth.bootstrap --query "test"',
        )
        return 1
    except NetworkError as exc:
        logger.error("network_error", telegram=True, error=str(exc), hint=_NETWORK_HINT)
        note_transient_error("threads", "network_error", cache)
        return _network_exit_code(exc)

    done = found = 0
    while done < max_profiles:
        handle = cache.spop(PROFILE_QUEUE_REDIS_KEY)
        if not handle:
            break
        if done:
            time.sleep(random.uniform(PROFILE_GAP_MIN_SECONDS, PROFILE_GAP_MAX_SECONDS))
        try:
            profile = client.fetch_profile(handle)
        except (SessionExpiredError, CheckpointRequiredError, RateLimitedError, NetworkError, ProfileQueryError) as exc:
            # Trả handle về hàng đợi trước khi dừng - lượt sau hỏi lại.
            cache.sadd(PROFILE_QUEUE_REDIS_KEY, handle)
            if isinstance(exc, RateLimitedError):
                logger.error("rate_limited", telegram=True, error=str(exc), handle=handle, done=done)
                note_transient_error("threads", "rate_limited", cache)
                return 1
            if isinstance(exc, NetworkError):
                logger.error("network_error", telegram=True, error=str(exc), handle=handle, hint=_NETWORK_HINT)
                note_transient_error("threads", "network_error", cache)
                return _network_exit_code(exc)
            if isinstance(exc, ProfileQueryError):
                # Thường là PROFILE_DOC_ID đã hết hạn - mọi handle sau cũng sẽ hỏng y vậy.
                logger.error("threads_profile_query_unexpected_response", telegram=True, error=str(exc), handle=handle)
                return 1
            logger.error(
                "checkpoint_required" if isinstance(exc, CheckpointRequiredError) else "session_expired",
                error=str(exc),
                handle=handle,
                done=done,
            )
            return 1
        cache.hset_json(PROFILE_RESULTS_REDIS_KEY, handle, _result(handle, profile))
        cache.expire(PROFILE_RESULTS_REDIS_KEY, PROFILE_RESULTS_TTL_SECONDS)
        done += 1
        found += profile is not None
        logger.info(
            "threads_profile_fetched",
            handle=handle,
            found=profile is not None,
            follower_count=(profile or {}).get("follower_count"),
        )

    logger.info("threads_profiles_finished", fetched=done, found=found)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--max", type=int, default=PROFILE_MAX_PER_RUN, help="Tối đa số handle mỗi lượt.")
    parser.add_argument("--username", help="Hỏi đúng một handle và in kết quả (không đọc/ghi hàng đợi).")
    parser.add_argument("--run-id", help="Do crawl_request_consumer.py đặt - gắn vào mọi dòng log của tiến trình này.")
    args = parser.parse_args()
    if args.run_id:
        bind_run_id(args.run_id)
    if args.username:
        profile = ThreadsGraphQLClient().fetch_profile(args.username)
        print(json.dumps(profile, ensure_ascii=False))
        return
    sys.exit(run(max(1, args.max)))


if __name__ == "__main__":
    main()
