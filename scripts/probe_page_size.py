"""Đo xem Facebook/Threads có chịu trả nhiều bài hơn trên một trang tìm kiếm khi xin count lớn hơn không.

Chỉ đọc - không publish Kafka, không ghi dedupe. Mỗi count gọi trang đầu + 1 trang kế (2 request), nghỉ giữa các lần
như bóp nhịp thường của client. Chạy trên máy có session (VPS), bằng user app:

    .venv/bin/python -m scripts.probe_page_size --query "Trại Buôn Người"
    .venv/bin/python -m scripts.probe_page_size --platform threads --counts 10,25,50
    .venv/bin/python -m scripts.probe_page_size --platform facebook --date-split 7   # lọc theo ngày ra thêm bài không?
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from social_crawler.clients.redis import RedisCache
from social_crawler.spiders.search_query import build_search_query


def _clients(platform: str):
    if platform == "facebook":
        from social_crawler.spiders.facebook.auth.graphql_client import FacebookGraphQLClient, find_page_info
        from social_crawler.spiders.facebook.features.search.extract import extract_response

        return FacebookGraphQLClient(redis_cache=RedisCache()), extract_response, find_page_info
    from social_crawler.spiders.threads.auth.graphql_client import ThreadsGraphQLClient, find_page_info
    from social_crawler.spiders.threads.features.search.extract import extract_response

    return ThreadsGraphQLClient(redis_cache=RedisCache()), extract_response, find_page_info


def probe(platform: str, query: str, counts: list[int], recent: bool = False) -> None:
    client, extract, page_info_of = _clients(platform)
    if recent:
        print("  mẫu recent =", repr((client._cache.get("variables_template") or {}).get("recent")), flush=True)
    search_query = build_search_query(query)
    for count in counts:
        sizes: list[int] = []
        cursor = None
        has_next = None
        for _ in range(2):
            response = (
                (client.search(search_query, count, None, True) if recent else client.search(search_query, count))
                if cursor is None
                else (
                    client.search_next_page(search_query, cursor, count, True)
                    if recent
                    else client.search_next_page(search_query, cursor, count)
                )
            )
            posts = extract(response)
            posts = posts[0] if isinstance(posts, tuple) else posts
            sizes.append(len(posts))
            info = page_info_of(response) or {}
            has_next, cursor = info.get("has_next_page"), info.get("end_cursor")
            if not (has_next and cursor):
                break
        print(f"{platform} count={count} recent={recent}: bài/trang={sizes} has_next={has_next}", flush=True)
        if recent and platform == "threads":
            # In ngày đăng để thấy tab "Gần đây" có thật sự xếp mới nhất trước không.
            print("  taken_at:", [p.get("timestamp") for p in posts][:10], flush=True)


def _post_ids(posts: list[dict]) -> set[str]:
    return {str(p.get("post_id") or p.get("id") or p.get("url")) for p in posts}


def _crawl_ids(client, extract, page_info_of, query: str, count: int, pages: int, day: date | None) -> tuple[set[str], int]:
    """Id bài của tối đa `pages` trang (một ngày đăng nếu có `day`) và số request đã dùng."""
    ids: set[str] = set()
    cursor = None
    used = 0
    for _ in range(pages):
        start, end = (day, day) if day else (None, None)
        response = (
            client.search(query, count, start, end)
            if cursor is None
            else client.search_next_page(query, cursor, count, start, end)
        )
        used += 1
        posts = extract(response)
        ids |= _post_ids(posts[0] if isinstance(posts, tuple) else posts)
        info = page_info_of(response) or {}
        cursor = info.get("end_cursor")
        if not (info.get("has_next_page") and cursor):
            break
    return ids, used


def probe_date_split(query: str, count: int, days: int, base_pages: int, day_pages: int) -> None:
    """Facebook: tìm không lọc (base_pages trang) so với chia theo từng ngày đăng của `days` ngày gần nhất (day_pages
    trang mỗi ngày) - kết quả tìm kiếm có độ sâu giới hạn, mỗi ngày là một tập kết quả riêng."""
    client, extract, page_info_of = _clients("facebook")
    search_query = build_search_query(query)
    base, base_used = _crawl_ids(client, extract, page_info_of, search_query, count, base_pages, None)
    print(f"facebook không lọc: {len(base)} bài / {base_used} request", flush=True)
    split: set[str] = set()
    split_used = 0
    for offset in range(days):
        day = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date() - timedelta(days=offset)
        ids, used = _crawl_ids(client, extract, page_info_of, search_query, count, day_pages, day)
        split |= ids
        split_used += used
        print(f"  {day}: {len(ids)} bài / {used} request", flush=True)
    print(
        f"facebook chia {days} ngày: {len(split)} bài / {split_used} request - mới so với không lọc: {len(split - base)}",
        flush=True,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--platform", choices=["facebook", "threads", "both"], default="both")
    parser.add_argument("--query", default="Trại Buôn Người")
    parser.add_argument("--counts", default="5,10,20,50")
    parser.add_argument("--recent", action="store_true", help='Threads: tab "Gần đây"')
    parser.add_argument("--date-split", type=int, metavar="DAYS", help="Facebook: so tìm không lọc với chia theo ngày")
    parser.add_argument("--base-pages", type=int, default=10, help="--date-split: số trang của lượt không lọc")
    parser.add_argument("--day-pages", type=int, default=3, help="--date-split: số trang mỗi ngày")
    args = parser.parse_args()
    if args.date_split:
        probe_date_split(args.query, int(args.counts.split(",")[0]), args.date_split, args.base_pages, args.day_pages)
        raise SystemExit(0)
    counts = [int(c) for c in args.counts.split(",")]
    for platform in ("facebook", "threads") if args.platform == "both" else (args.platform,):
        try:
            probe(platform, args.query, counts, args.recent and platform == "threads")
        except Exception as exc:  # noqa: BLE001 - lỗi một nền tảng không chặn nền tảng kia
            print(f"{platform} lỗi: {type(exc).__name__}: {exc}")
