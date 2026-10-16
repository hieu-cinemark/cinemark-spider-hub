"""Đo xem Facebook/Threads có chịu trả nhiều bài hơn trên một trang tìm kiếm khi xin count lớn hơn không.

Chỉ đọc - không publish Kafka, không ghi dedupe. Mỗi count gọi trang đầu + 1 trang kế (2 request), nghỉ giữa các lần
như bóp nhịp thường của client. Chạy trên máy có session (VPS), bằng user app:

    .venv/bin/python -m scripts.probe_page_size --query "Trại Buôn Người"
    .venv/bin/python -m scripts.probe_page_size --platform threads --counts 10,25,50
"""

from __future__ import annotations

import argparse

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


def probe(platform: str, query: str, counts: list[int]) -> None:
    client, extract, page_info_of = _clients(platform)
    search_query = build_search_query(query)
    for count in counts:
        sizes: list[int] = []
        cursor = None
        has_next = None
        for _ in range(2):
            response = (
                client.search(search_query, count)
                if cursor is None
                else client.search_next_page(search_query, cursor, count)
            )
            posts = extract(response)
            posts = posts[0] if isinstance(posts, tuple) else posts
            sizes.append(len(posts))
            info = page_info_of(response) or {}
            has_next, cursor = info.get("has_next_page"), info.get("end_cursor")
            if not (has_next and cursor):
                break
        print(f"{platform} count={count}: bài/trang={sizes} has_next={has_next}", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--platform", choices=["facebook", "threads", "both"], default="both")
    parser.add_argument("--query", default="Trại Buôn Người")
    parser.add_argument("--counts", default="5,10,20,50")
    args = parser.parse_args()
    counts = [int(c) for c in args.counts.split(",")]
    for platform in ("facebook", "threads") if args.platform == "both" else (args.platform,):
        try:
            probe(platform, args.query, counts)
        except Exception as exc:  # noqa: BLE001 - lỗi một nền tảng không chặn nền tảng kia
            print(f"{platform} lỗi: {type(exc).__name__}: {exc}")
