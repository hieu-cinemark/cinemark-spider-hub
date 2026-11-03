"""Đọc feed Page công khai bằng phiên guest HTTP, không dùng cookie/token tài khoản.

HTML thường chỉ nhúng một bài. Giữ cookie guest và LSD của chính HTML đó để gọi
ProfileCometTimelineFeedRefetchQuery theo cursor; Doc ID lấy từ bundle công khai.
"""

from __future__ import annotations

import html as html_lib
import json
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from curl_cffi import requests as cffi_requests

from social_crawler.core.logger import get_logger
from social_crawler.spiders.common.comet_graphql_client import _parse_graphql_response
from social_crawler.spiders.facebook.auth.http_refresh import classify_error, extract_tokens, operations_in_script
from social_crawler.spiders.facebook.features.post_metrics.post_metrics import (
    CURL_CFFI_IMPERSONATE_TARGET,
    HEADERS,
    REQUEST_TIMEOUT_SECONDS,
)
from social_crawler.spiders.facebook.features.search.extract import extract_response
from social_crawler.spiders.facebook.response_utils import iter_matching

BLOCKED_STATUSES = {"login_redirect", "timeout", "http_error", "no_data"}
_JSON_SCRIPT = re.compile(r'<script type="application/json"[^>]*>(.*?)</script>', re.DOTALL)
FEED_QUERY = "ProfileCometTimelineFeedRefetchQuery"
_FEED_FIELD = "timeline_list_feed_units"
_MAX_ASSETS = 30
logger = get_logger(__name__)


@dataclass
class PublicPage:
    status: str  # ok | unavailable | login_redirect | no_data | timeout | http_error
    http_status: int
    info: dict[str, Any] = field(default_factory=dict)
    posts: list[dict[str, Any]] = field(default_factory=list)
    pages_fetched: int = 0
    stop_reason: str = ""
    pagination_status: str | None = None
    since_ts: int | None = None
    until_ts: int | None = None


def _json_scripts(html: str):
    for match in _JSON_SCRIPT.finditer(html):
        try:
            yield json.loads(match.group(1))
        except ValueError:
            continue


def _count(pattern: str, text: str) -> int | None:
    match = re.search(pattern, text)
    digits = re.sub(r"\D", "", match.group(1)) if match else ""
    return int(digits) if digits else None


def _first(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text)
    return match.group(1) if match else None


def parse_info(html: str) -> dict[str, Any]:
    description = html_lib.unescape(_first(r'<meta property="og:description" content="([^"]*)"', html) or "")
    return {
        "name": html_lib.unescape(_first(r'<meta property="og:title" content="([^"]*)"', html) or ""),
        "page_id": _first(r'"userID":"(\d+)"', html),
        "vanity": _first(r'"userVanity":"([^"]+)"', html),
        "followers": _count(r"([\d.,]+)\s*(?:người theo dõi|followers)", description),
        "talking_about": _count(r"([\d.,]+)\s*(?:người đang nói|talking about)", description),
        "category": _first(r'"category_name":"([^"]+)"', html),
        "verified": _first(r'"is_verified":(true|false)', html) == "true",
    }


def parse_posts(html: str) -> list[dict[str, Any]]:
    posts: dict[str, dict[str, Any]] = {}
    for blob in _json_scripts(html):
        for post in extract_response(blob if isinstance(blob, dict) and "data" in blob else {"data": blob})[0]:
            if post.get("post_id"):
                posts.setdefault(str(post["post_id"]), post)
    return list(posts.values())


def _response_status(response, url: str) -> str:
    final_url = str(response.url or url)
    location = response.headers.get("location", "")
    if any(marker in value for value in (final_url, location) for marker in ("/login", "/checkpoint")):
        return "login_redirect"
    if response.status_code == 404:
        return "unavailable"
    if response.status_code != 200:
        return "http_error"
    return "ok"


def _initial_feed(html: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    recipe = page_info = None
    for blob in _json_scripts(html):
        for node in iter_matching(blob, lambda n: n.get("queryName") == "ProfileCometTimelineFeedQuery"):
            if str(node.get("actorID")) == "0" and isinstance(node.get("variables"), dict):
                recipe = node
        for node in iter_matching(blob, lambda n: isinstance(n.get(_FEED_FIELD), dict)):
            if isinstance(node[_FEED_FIELD].get("page_info"), dict):
                page_info = node[_FEED_FIELD]["page_info"]
        # Relay sends pagination in a deferred chunk, separate from the feed edges.
        for node in iter_matching(blob, lambda n: isinstance(n.get("path"), list)):
            if node["path"] == ["user", _FEED_FIELD] and isinstance(node.get("data"), dict):
                deferred_info = node["data"].get("page_info")
                if isinstance(deferred_info, dict):
                    page_info = deferred_info
    return recipe, page_info


def _feed_doc_id(session, html: str, tokens: dict[str, str], cache=None) -> str | None:
    override = os.getenv("FB_PUBLIC_PAGES_DOC_ID")
    key = f"facebook:public_pages:feed_doc:{tokens.get('__rev', 'unknown')}"
    cached = override or (cache.get(key) if cache else None)
    if isinstance(cached, str) and cached.isascii() and cached.isdigit():
        return cached
    sources = re.findall(r'<script\b[^>]*\bsrc=["\']([^"\']+)', html)
    sources += re.findall(r'"src"\s*:\s*"(https:[^"]+\.js[^" ]*)"', html)
    sources = list(dict.fromkeys(html_lib.unescape(src.replace("\\/", "/")) for src in sources))
    doc_id = operations_in_script(html).get(FEED_QUERY)
    fetched = 0
    deadline = time.monotonic() + 75
    for src in sources:
        remaining = deadline - time.monotonic()
        if doc_id or fetched >= _MAX_ASSETS or remaining <= 0:
            break
        parsed = urlsplit(src)
        if (
            parsed.scheme != "https"
            or parsed.port not in (None, 443)
            or parsed.username
            or parsed.password
            or not (parsed.hostname or "").endswith(".fbcdn.net")
        ):
            continue
        fetched += 1
        try:
            response = session.get(src, timeout=min(15, remaining), allow_redirects=False)
        except cffi_requests.RequestsError:
            continue
        if response.status_code == 200 and len(response.content) <= 5_000_000:
            doc_id = operations_in_script(response.text).get(FEED_QUERY)
    if doc_id and cache:
        cache.set(key, doc_id, ttl_seconds=86400)
    return doc_id


def _feed_page(text: str, page_id: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Only timeline pagination counts; comment/photo connections must never stop the feed."""
    text = re.sub(r"^\s*for\s*\(\s*;;\s*\)\s*;", "", text)
    data = _parse_graphql_response(text)
    if classify_error(data) == "doc_id":
        raise ValueError("doc_id_stale")
    if not isinstance(data, dict) or data.get("error"):
        raise ValueError("public_feed_unavailable")
    root = data.get("data")
    node = root.get("node") if isinstance(root, dict) else None
    connection = node.get(_FEED_FIELD) if isinstance(node, dict) and str(node.get("id")) == page_id else None
    if not isinstance(connection, dict) or not isinstance(connection.get("edges"), list):
        raise TypeError("public_feed_schema_changed")
    info = connection.get("page_info")
    if not isinstance(info, dict) or not isinstance(info.get("has_next_page"), bool):
        raise TypeError("public_feed_pagination_missing")
    if info["has_next_page"] and not info.get("end_cursor"):
        raise ValueError("public_feed_cursor_missing")
    # Logged-out optional fields (e.g. save_info) can error while feed data remains usable.
    # Root/connection failures must not masquerade as successful exhaustion.
    for error in data.get("errors") or []:
        path = error.get("path") if isinstance(error, dict) else None
        # @stream errors can use paths relative to a Story, rather than the root query.
        if not isinstance(path, list) or path[-2:] != ["save_info", "save_flow"]:
            raise ValueError("public_feed_query_error")
    return [post for post in extract_response({"data": connection})[0] if post.get("post_id")], info


def _in_window(post: dict[str, Any], since_ts: int, until_ts: int) -> bool:
    timestamp = post.get("timestamp")
    return isinstance(timestamp, (int, float)) and since_ts <= timestamp <= until_ts


def fetch_public_page(
    url: str, proxy: str, *, max_pages: int = 0, since_days: int = 60, cache=None, should_stop=None
) -> PublicPage:
    """An isolated guest session, bounded by publication date; max_pages=0 has no page limit. should_stop is checked before
    each feed request, so a long job honours Stop and keeps what it already fetched."""
    if max_pages < 0:
        raise ValueError("max_pages must be nonnegative")
    if since_days <= 0:
        raise ValueError("since_days must be positive")
    until_ts = int(time.time())
    since_ts = until_ts - since_days * 86400
    with cffi_requests.Session(
        impersonate=CURL_CFFI_IMPERSONATE_TARGET, proxies={"http": proxy, "https": proxy}, trust_env=False
    ) as session:
        try:
            response = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
        except cffi_requests.RequestsError:
            return PublicPage("timeout", 0)
        status = _response_status(response, url)
        if status != "ok":
            return PublicPage(status, response.status_code)
        html = response.text or ""
        info = parse_info(html)
        if not info["name"]:
            return PublicPage("no_data", 200)
        initial_posts = parse_posts(html)
        result = PublicPage(
            "ok",
            200,
            info,
            [p for p in initial_posts if _in_window(p, since_ts, until_ts)],
            pages_fetched=1,
            since_ts=since_ts,
            until_ts=until_ts,
        )
        recipe, page_info = _initial_feed(html)
        if not page_info:
            result.stop_reason = "pagination_missing"
            result.pagination_status = "schema_changed"
            return result
        if page_info.get("has_next_page") is False:
            result.stop_reason = "exhausted"
            return result
        if max_pages == 1:
            result.stop_reason = "max_pages"
            return result
        tokens = extract_tokens(html)
        if not recipe or str(recipe["variables"].get("userID")) != info["page_id"] or not tokens.get("lsd"):
            result.stop_reason = "guest_recipe_missing"
            result.pagination_status = "schema_changed"
            return result
        doc_id = _feed_doc_id(session, html, tokens, cache)
        if not doc_id:
            result.stop_reason = "public_doc_id_missing"
            result.pagination_status = "schema_changed"
            return result
        variables = {**recipe["variables"], "id": info["page_id"], "count": 10, "stream_count": 1}
        variables.pop("userID", None)
        variables.pop("runWithContinuationKey", None)
        variables.update(
            run_with_continuation_key=False,
            useDefaultActor=False,
            omitPinnedPost=True,
            afterTime=since_ts - 1,
            beforeTime=until_ts + 1,
        )
        body = {
            **tokens,
            "av": "0",
            "__user": "0",
            "__a": "1",
            "__comet_req": "15",
            "fb_dtsg": "",
            "jazoest": "2" + str(sum(map(ord, tokens["lsd"]))),
            "fb_api_caller_class": "RelayModern",
            "fb_api_req_friendly_name": FEED_QUERY,
            "server_timestamps": "true",
            "doc_id": doc_id,
        }
        headers = {
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded",
            "origin": "https://www.facebook.com",
            "referer": url,
            "x-fb-friendly-name": FEED_QUERY,
            "x-fb-lsd": tokens["lsd"],
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        posts = {str(p["post_id"]): p for p in result.posts}
        seen_ids = {str(p["post_id"]) for p in initial_posts}
        cursors = set()
        # Changing time filters changes the connection: never reuse the unfiltered HTML cursor.
        cursor = None
        while not max_pages or result.pages_fetched < max_pages:
            if should_stop and should_stop():
                result.stop_reason = "cancelled"
                break
            if cursor in cursors or (cursor is not None and (not isinstance(cursor, str) or not cursor)):
                result.stop_reason = "missing_or_repeated_cursor"
                break
            cursors.add(cursor)
            time.sleep(max(1, float(os.getenv("FB_PUBLIC_PAGES_REQUEST_INTERVAL", "5"))))
            try:
                response = session.post(
                    "https://www.facebook.com/api/graphql/",
                    headers=headers,
                    data={
                        **body,
                        "__req": format(result.pages_fetched, "x"),
                        "variables": json.dumps({**variables, "cursor": cursor}),
                    },
                    timeout=REQUEST_TIMEOUT_SECONDS,
                    allow_redirects=False,
                )
            except cffi_requests.RequestsError:
                result.pagination_status = result.stop_reason = "timeout"
                break
            result.http_status = response.status_code
            status = _response_status(response, url)
            if status != "ok":
                result.pagination_status = result.stop_reason = status
                break
            try:
                batch, page_info = _feed_page(response.text, info["page_id"])
            except (TypeError, ValueError, RuntimeError) as exc:
                result.pagination_status = result.stop_reason = (
                    "doc_id_stale" if str(exc) == "doc_id_stale" else "schema_changed"
                )
                if result.stop_reason == "doc_id_stale" and cache:
                    cache.delete(f"facebook:public_pages:feed_doc:{tokens.get('__rev', 'unknown')}")
                break
            result.pages_fetched += 1
            new_ids = {str(p["post_id"]) for p in batch} - seen_ids
            seen_ids.update(str(p["post_id"]) for p in batch)
            for post in batch:
                if _in_window(post, since_ts, until_ts):
                    posts.setdefault(str(post["post_id"]), post)
            result.posts = list(posts.values())
            logger.info(
                "public_page_feed_page",
                page_url=url,
                page=result.pages_fetched,
                fetched=len(batch),
                posts_in_window=len(result.posts),
                since_ts=since_ts,
            )
            if not page_info["has_next_page"]:
                result.stop_reason = "exhausted"
                break
            if batch and all(isinstance(p.get("timestamp"), (int, float)) and p["timestamp"] < since_ts for p in batch):
                result.stop_reason = "since_days"
                break
            if not new_ids and cursor is not None:
                result.stop_reason = "no_new_posts"
                break
            cursor = page_info.get("end_cursor")
            if not isinstance(cursor, str) or not cursor:
                result.stop_reason = "missing_or_repeated_cursor"
                break
        if not result.stop_reason:
            result.stop_reason = "max_pages"
        return result
