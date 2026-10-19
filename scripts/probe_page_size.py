"""Đo xem Facebook/Threads có chịu trả nhiều bài hơn trên một trang tìm kiếm khi xin count lớn hơn không.

Chỉ đọc - không publish Kafka, không ghi dedupe. Mỗi count gọi trang đầu + 1 trang kế (2 request), nghỉ giữa các lần
như bóp nhịp thường của client. Chạy trên máy có session (VPS), bằng user app:

    .venv/bin/python -m scripts.probe_page_size --query "Trại Buôn Người"
    .venv/bin/python -m scripts.probe_page_size --platform threads --counts 10,25,50
    .venv/bin/python -m scripts.probe_page_size --platform facebook --date-split 7   # lọc theo ngày ra thêm bài không?
    .venv/bin/python -m scripts.probe_page_size --variants                            # tab / exact_match / locale / bsid
    .venv/bin/python -m scripts.probe_page_size --dump-filters                        # bộ lọc Facebook trả về (fetch_filters)
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from social_crawler.clients.redis import RedisCache
from social_crawler.spiders.keyword_filter import mentions_keyword
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


def _fb_search(client, query: str, cursor: str | None, extra: dict) -> dict:
    """Một trang tìm kiếm Facebook với các ghi đè thêm (key bất kỳ trong variables_template, vd. type / exact_match)."""
    from social_crawler.spiders.facebook.auth.graphql_client import _search_overrides

    overrides = {**_search_overrides(query, cursor, 5, None, None), **extra}
    return client._run(
        doc_id=client._cache["doc_id"],
        friendly_name=client._cache["fb_api_req_friendly_name"],
        template=client._cache.get("variables_template"),
        template_source="variables_template",
        overrides=overrides,
    )


def _filter(name: str, args: object = "") -> str:
    return json.dumps({"name": name, "args": args if isinstance(args, str) else json.dumps(args, separators=(",", ":"))}, separators=(",", ":"))


def _last_days(days: int) -> list[str]:
    from social_crawler.spiders.facebook.auth.graphql_client import _build_date_filters

    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).date()
    return _build_date_filters(today - timedelta(days=days - 1), today)


VARIANTS: dict[str, dict] = {
    "baseline (POSTS_TAB)": {},
    "bsid mới": {"bsid": "__fresh__"},
    "exact_match": {"exact_match": True},
    "locale vi_VN": {"locale": "vi_VN"},
    "VIDEOS_TAB": {"type": "VIDEOS_TAB"},
    "VIDEOS_TAB + mới đây": {"type": "VIDEOS_TAB", "filters": [_filter("recent_posts")]},
    "VIDEOS_TAB + 7 ngày": {"type": "VIDEOS_TAB", "filters": "__last7__"},
    "POSTS + công khai": {"filters": "__public__"},
    "REELS_TAB": {"type": "REELS_TAB"},
    "PHOTOS_TAB": {"type": "PHOTOS_TAB"},
}
# Tên bộ lọc "Bài viết công khai" - đặt bằng --public-filter sau khi xem --dump-filters (mặc định là đoán).
PUBLIC_FILTER = "public_posts"


def probe_variants(query: str, pages: int, names: list[str] | None) -> None:
    """Mỗi biến thể: số bài / số bài mới so với baseline / số request - đánh giá theo bài mới trên mỗi request."""
    import uuid

    client, extract, page_info_of = _clients("facebook")
    search_query = build_search_query(query)
    seen_base: set[str] = set()
    for name, extra in VARIANTS.items():
        if names and name not in names:
            continue
        special = {"__fresh__": lambda: str(uuid.uuid4()), "__last7__": lambda: _last_days(7), "__public__": lambda: [_filter(PUBLIC_FILTER)]}
        extra = {k: (special[v]() if isinstance(v, str) and v in special else v) for k, v in extra.items()}
        ids: set[str] = set()
        matched, ages, videos = 0, [], 0
        cursor, used, error = None, 0, ""
        try:
            for _ in range(pages):
                response = _fb_search(client, search_query, cursor, extra)
                used += 1
                posts = extract(response)
                posts = posts[0] if isinstance(posts, tuple) else posts
                for post in posts:
                    if _post_ids([post]) <= ids:
                        continue
                    matched += mentions_keyword(post.get("message"), query)
                    videos += post.get("media_type") in ("video", "Video", "reel")
                    if isinstance(post.get("timestamp"), int):
                        ages.append((time.time() - post["timestamp"]) / 86400)
                    ids |= _post_ids([post])
                info = page_info_of(response) or {}
                cursor = info.get("end_cursor")
                if not (info.get("has_next_page") and cursor):
                    break
        except Exception as exc:  # noqa: BLE001 - biến thể không hợp lệ thì ghi lại rồi thử cái kế
            error = f" lỗi: {type(exc).__name__}: {str(exc)[:120]}"
        new = len(ids - seen_base) if seen_base else len(ids)
        if name.startswith("baseline"):
            seen_base = ids
        median_age = sorted(ages)[len(ages) // 2] if ages else None
        quality = f" · khớp từ khoá {matched}/{len(ids)} · video {videos} · tuổi trung vị {median_age:.0f} ngày" if ids and median_age is not None else ""
        print(f"{name:24} {len(ids):4} bài · {new:4} mới so với baseline · {used} request{quality}{error}", flush=True)


def dump_filters(query: str) -> None:
    """In mọi khối có 'filter' trong tên key của response trang đầu (fetch_filters=true) - tên và args bộ lọc."""
    client, _, _ = _clients("facebook")
    response = _fb_search(client, build_search_query(query), None, {})
    found: list[tuple[str, object]] = []

    def walk(node: object, path: str) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                here = f"{path}.{key}"
                if "filter" in key.lower() and value not in (None, [], {}):
                    found.append((here, value))
                walk(value, here)
        elif isinstance(node, list):
            for index, item in enumerate(node[:50]):
                walk(item, f"{path}[{index}]")

    walk(response, "")
    groups = [value for path, value in found if path.endswith(".results.filters") and isinstance(value, list)]
    for group in groups:
        for block in group:
            for item in block.get("filters") or []:
                main = item.get("main_filter") or {}
                edges = (main.get("filter_values") or {}).get("edges") or []
                print(f"- {main.get('name')} ({main.get('pill_button_type')}): {main.get('text')}", flush=True)
                for edge in edges[:8]:
                    node = edge.get("node") or {}
                    print(f"    {node.get('text')!r:40} value={node.get('value')}", flush=True)
    if not groups:
        for path, value in found[:40]:
            print(path, "=", json.dumps(value, ensure_ascii=False)[:400], flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--platform", choices=["facebook", "threads", "both"], default="both")
    parser.add_argument("--query", default="Trại Buôn Người")
    parser.add_argument("--counts", default="5,10,20,50")
    parser.add_argument("--recent", action="store_true", help='Threads: tab "Gần đây"')
    parser.add_argument("--date-split", type=int, metavar="DAYS", help="Facebook: so tìm không lọc với chia theo ngày")
    parser.add_argument("--base-pages", type=int, default=10, help="--date-split: số trang của lượt không lọc")
    parser.add_argument("--day-pages", type=int, default=3, help="--date-split: số trang mỗi ngày")
    parser.add_argument("--variants", action="store_true", help="Facebook: so tab / exact_match / locale / bsid mới")
    parser.add_argument("--only", help="--variants: chỉ các biến thể này, cách nhau dấu phẩy")
    parser.add_argument("--pages", type=int, default=5, help="--variants: số trang mỗi biến thể")
    parser.add_argument("--dump-filters", action="store_true", help="Facebook: in bộ lọc trong response")
    parser.add_argument("--public-filter", default=PUBLIC_FILTER, help="tên bộ lọc 'Bài viết công khai' (xem --dump-filters)")
    args = parser.parse_args()
    PUBLIC_FILTER = args.public_filter
    if args.variants:
        probe_variants(args.query, args.pages, args.only.split(",") if args.only else None)
        raise SystemExit(0)
    if args.dump_filters:
        dump_filters(args.query)
        raise SystemExit(0)
    if args.date_split:
        probe_date_split(args.query, int(args.counts.split(",")[0]), args.date_split, args.base_pages, args.day_pages)
        raise SystemExit(0)
    counts = [int(c) for c in args.counts.split(",")]
    for platform in ("facebook", "threads") if args.platform == "both" else (args.platform,):
        try:
            probe(platform, args.query, counts, args.recent and platform == "threads")
        except Exception as exc:  # noqa: BLE001 - lỗi một nền tảng không chặn nền tảng kia
            print(f"{platform} lỗi: {type(exc).__name__}: {exc}")
