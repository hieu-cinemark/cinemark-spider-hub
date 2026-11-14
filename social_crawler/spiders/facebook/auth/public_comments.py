"""Bình luận công khai ẩn danh, chuyển thể từ bsho5/fbgql (MIT; licenses/fbgql-MIT.txt).

Tham chiếu: 28e03ae84012be44d37a67014fcfcfcc420c9ee9, Doc ID tháng 7/2026.
Không chọn tài khoản, không cookie/token tài khoản, trình duyệt, giả vân tay hay dự phòng đăng nhập.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
import uuid
from datetime import UTC, datetime

import requests

from social_crawler.clients.redis import RedisCache
from social_crawler.spiders.common.comet_graphql_client import _parse_graphql_response
from social_crawler.spiders.facebook.auth.http_refresh import public_proxy

GRAPHQL_URL = "https://www.facebook.com/api/graphql/"
OPERATIONS = {"comments": "CommentsListComponentsPaginationQuery", "replies": "Depth1CommentsListPaginationQuery"}
DEFAULT_DOC_IDS = {"comments": "27806180149070312", "replies": "27888228590762910"}
PROVIDERS = {
    "__relay_internal__pv__CometUFICommentAutoTranslationTyperelayprovider": "ORIGINAL",
    "__relay_internal__pv__CometUFICommentAvatarStickerAnimatedImagerelayprovider": False,
    "__relay_internal__pv__CometUFICommentActionLinksRewriteEnabledrelayprovider": False,
    "__relay_internal__pv__IsWorkUserrelayprovider": False,
}


class PublicCommentsError(RuntimeError):
    pass


class PublicTemporaryError(PublicCommentsError):
    pass


class PublicQueryStale(PublicCommentsError):
    pass


class PublicSchemaChanged(PublicCommentsError):
    pass


class PublicAccessUnavailable(PublicCommentsError):
    pass


def public_mode(request: dict) -> bool:
    # Job cũ vẫn là job chỉ định tài khoản. Không bao giờ biến phiên đăng nhập đã chết thành ẩn danh.
    return (
        request.get("platform") == "facebook"
        and request.get("type") == "comments"
        and request.get("access_mode") == "anonymous"
    )


def numeric_post_id(value: str) -> str:
    value = str(value)
    if value.isascii() and value.isdigit():
        return value
    parts = value.split("_")
    if len(parts) == 2 and all(p.isascii() and p.isdigit() for p in parts):
        return parts[1]
    raise PublicCommentsError("Anonymous comments require the numeric post ID recorded in the database")


def payload(kind, identifier, doc_id, cursor=None, expansion_token=None):
    variables = {
        "feedLocation": "POST_PERMALINK_DIALOG",
        "focusCommentID": None,
        "scale": 2,
        "useDefaultActor": False,
        "id": identifier,
        **PROVIDERS,
    }
    if kind == "comments":
        variables.update(
            commentsAfterCount=-1,
            commentsAfterCursor=cursor,
            commentsBeforeCount=None,
            commentsBeforeCursor=None,
            commentsIntentToken="REVERSE_CHRONOLOGICAL_UNFILTERED_INTENT_V1",
        )
    elif kind == "replies":
        variables.update(
            clientKey=None,
            expansionToken=expansion_token,
            repliesAfterCount=-1 if cursor else None,
            repliesAfterCursor=cursor,
            repliesBeforeCount=None,
            repliesBeforeCursor=None,
        )
    else:
        raise ValueError("Unsupported public operation")
    return {
        "av": "0",
        "__user": "0",
        "__a": "1",
        "fb_dtsg": "",
        "doc_id": doc_id,
        "variables": json.dumps(variables, separators=(",", ":")),
    }


def parse_page(text, kind):
    if text.lstrip().startswith("for(;;);"):
        text = text.lstrip().removeprefix("for(;;);")
    try:
        data = _parse_graphql_response(text)
    except ValueError, RuntimeError:
        raise PublicSchemaChanged("Public GraphQL returned non-JSON data") from None
    if not isinstance(data, dict):
        raise PublicSchemaChanged("Public GraphQL response is not an object")
    if data.get("error"):
        raise PublicAccessUnavailable(f"Public GraphQL refused the request (code={data['error']}); no login fallback")
    errors = data.get("errors") or []
    critical = [e for e in errors if isinstance(e, dict) and str(e.get("severity", "")).upper() != "WARNING"]
    for error in critical:
        message = str(error.get("message") or "").lower()
        if "missing_required_variable_value" in message or "variable" in message and "invalid value" in message:
            raise PublicSchemaChanged("Public query variables no longer match the server")
        if "graphql document with id" in message and "was not found" in message:
            raise PublicQueryStale("Public Doc ID no longer resolves; update registry, never bootstrap an account")
    if critical:
        if all(str(e.get("code")) == "1675012" for e in critical):
            return None  # Thử lại có giới hạn cùng con trỏ; không bao giờ coi là đã hết bài.
        raise PublicCommentsError("Public GraphQL returned critical errors")
    node = (data.get("data") or {}).get("node")
    if not isinstance(node, dict):
        raise PublicAccessUnavailable("Post/comment is unavailable to the anonymous actor")
    if kind == "comments":
        connection = (node.get("comment_rendering_instance_for_feed_location") or {}).get("comments")
    else:
        connection = node.get("replies_connection")
    if not isinstance(connection, dict) or not isinstance(connection.get("edges"), list):
        raise PublicSchemaChanged("Expected public comment connection is missing")
    info = connection.get("page_info")
    if not isinstance(info, dict) or not isinstance(info.get("has_next_page"), bool):
        raise PublicSchemaChanged("Public comment pagination metadata is missing")
    if info["has_next_page"] and (not info.get("end_cursor") or not connection["edges"]):
        raise PublicSchemaChanged("Public pagination claims another page without usable data/cursor")
    nodes = []
    for edge in connection["edges"]:
        if not isinstance(edge, dict) or not isinstance(edge.get("node"), dict):
            raise PublicSchemaChanged("Invalid public comment edge")
        nodes.append(edge["node"])
    return nodes, info


def queue_hold_reason(cache=None, proxy_id=None) -> str | None:
    """Lý do hiện không job công khai nào chạy được, để worker giữ hàng đợi thay vì làm lỗi từng job."""
    proxy_id = int(proxy_id if proxy_id is not None else os.getenv("FACEBOOK_PUBLIC_COMMENTS_PROXY_ID", "10"))
    try:
        public_proxy(proxy_id)
    except Exception:  # noqa: BLE001 - disabled, assigned to an account, or unreadable: all block every job
        return "proxy_unavailable"
    cache = cache or RedisCache()
    scope = f"facebook:public_comments:proxy:{proxy_id}"
    if cache.get(f"{scope}:cooldown"):
        return "cooldown"
    day = datetime.now(UTC).date().isoformat()
    if int(cache.get(f"{scope}:requests:{day}") or 0) >= int(os.getenv("FACEBOOK_PUBLIC_COMMENTS_DAILY_CAP", "400")):
        return "daily_budget"
    return None


class PublicCommentsClient:
    def __init__(self, *, cache=None, proxy_id=None):
        self.cache = cache or RedisCache()
        self.proxy_id = int(proxy_id if proxy_id is not None else os.getenv("FACEBOOK_PUBLIC_COMMENTS_PROXY_ID", "10"))
        proxy = public_proxy(self.proxy_id)  # Từ chối mọi endpoint đã gán cho bất kỳ tài khoản nào, kể cả tài khoản đang tắt.
        self.proxy_url = proxy
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.proxies = {"http": proxy, "https": proxy}
        self.interval = max(1, float(os.getenv("FACEBOOK_PUBLIC_COMMENTS_INTERVAL", "5")))
        self.daily_cap = max(1, int(os.getenv("FACEBOOK_PUBLIC_COMMENTS_DAILY_CAP", "400")))
        self.max_requests = max(1, int(os.getenv("FACEBOOK_PUBLIC_COMMENTS_MAX_REQUESTS", "400")))
        self.requests = 0
        self.doc_ids = {}
        registry = self.cache.get("facebook:public_comment_doc_ids") or {}
        discovered = (self.cache.get("facebook:public_operations:v1") or {}).get("operations", {})
        for kind, name in OPERATIONS.items():
            value = (
                os.getenv(f"FBGQL_DOC_ID_{kind.upper()}")
                or registry.get(kind)
                or discovered.get(name)
                or DEFAULT_DOC_IDS[kind]
            )
            if not isinstance(value, str) or not value.isascii() or not value.isdigit():
                self.close()
                raise PublicCommentsError("Invalid public Doc ID configuration")
            self.doc_ids[kind] = value

    def close(self):
        self.session.close()

    def _post(self, kind, body):
        scope = f"facebook:public_comments:proxy:{self.proxy_id}"
        lock, owner = f"{scope}:lock", uuid.uuid4().hex
        if not self.cache.set_nx(lock, owner, ttl_seconds=120):
            raise PublicCommentsError("Public proxy is busy or its lease cannot be verified")
        try:
            if public_proxy(self.proxy_id) != self.proxy_url:
                raise PublicAccessUnavailable("Public proxy configuration changed during the crawl")
            if self.cache.get(f"{scope}:cooldown"):
                raise PublicAccessUnavailable("Public proxy is in cooldown; no proxy rotation or account fallback")
            if self.requests >= self.max_requests:
                raise PublicCommentsError("Public job request budget exhausted before completion")
            day = datetime.now(UTC).date().isoformat()
            used = self.cache.next_counter(f"{scope}:requests:{day}", 0, 2 * 86400)
            if used > self.daily_cap:
                raise PublicCommentsError("Public proxy daily budget exhausted before completion")
            last = float(self.cache.get(f"{scope}:last_request") or 0)
            delay = self.interval - (time.time() - last)
            if delay > 0:
                time.sleep(min(delay, self.interval))
            self.cache.set(f"{scope}:last_request", time.time(), ttl_seconds=86400)
            self.requests += 1
            self.session.cookies.clear()
            try:
                response = self.session.post(
                    GRAPHQL_URL,
                    data=body,
                    timeout=20,
                    allow_redirects=False,
                    headers={
                        "user-agent": "Mozilla/5.0",
                        "content-type": "application/x-www-form-urlencoded",
                        "origin": "https://www.facebook.com",
                        "referer": "https://www.facebook.com/",
                        "x-fb-friendly-name": OPERATIONS[kind],
                    },
                )
            except requests.RequestException:
                raise PublicTemporaryError("Public request network failure; no account was used") from None
            if response.status_code in (403, 429):
                self.cache.set(f"{scope}:cooldown", 1, ttl_seconds=1800)
                raise PublicAccessUnavailable(f"Public access blocked (HTTP {response.status_code})")
            if response.status_code in (500, 502, 503, 504):
                raise PublicTemporaryError(f"Public server temporarily unavailable (HTTP {response.status_code})")
            if response.status_code != 200:
                raise PublicAccessUnavailable(f"Public request failed (HTTP {response.status_code})")
            return response.text
        finally:
            self.cache.delete_if_equals(lock, owner)

    def page(self, kind, identifier, cursor=None, expansion_token=None):
        body = payload(kind, identifier, self.doc_ids[kind], cursor, expansion_token)
        for attempt in range(3):
            try:
                result = parse_page(self._post(kind, body), kind)
            except PublicTemporaryError:
                if attempt >= 2:
                    self.cache.set(f"facebook:public_comments:proxy:{self.proxy_id}:cooldown", 1, ttl_seconds=300)
                    raise
                time.sleep(3 * (attempt + 1))
                continue
            except (PublicQueryStale, PublicSchemaChanged) as exc:
                self.cache.set(
                    f"facebook:public_comment_doc_validation:{kind}",
                    {
                        "doc_id": self.doc_ids[kind],
                        "valid": False,
                        "checked_at": time.time(),
                        "error": type(exc).__name__,
                    },
                    ttl_seconds=86400,
                )
                # Mọi job sau cũng sẽ lỗi y vậy cho tới khi sửa query.
                self.cache.set(f"facebook:public_comments:proxy:{self.proxy_id}:cooldown", 1, ttl_seconds=1800)
                raise
            if result is not None:
                self.cache.set(
                    f"facebook:public_comment_doc_validation:{kind}",
                    {"doc_id": self.doc_ids[kind], "valid": True, "validated_at": time.time()},
                    ttl_seconds=86400,
                )
                return result
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
        self.cache.set(f"facebook:public_comments:proxy:{self.proxy_id}:cooldown", 1, ttl_seconds=300)
        raise PublicCommentsError("Public pagination repeatedly returned 1675012; crawl is incomplete")

    def comments(self, post_id, cursor=None):
        feedback = base64.b64encode(f"feedback:{numeric_post_id(post_id)}".encode()).decode()
        return self.page("comments", feedback, cursor)

    def replies(self, feedback_id, expansion_token, cursor=None):
        return self.page("replies", feedback_id, cursor, expansion_token)


def main():
    parser = argparse.ArgumentParser(
        description="Validate one anonymous comment page; never publish data or use an account"
    )
    parser.add_argument("--post-id", required=True)
    parser.add_argument("--proxy-id", type=int)
    parser.add_argument(
        "--check-replies", action="store_true", help="Probe one reply page if the comment page provides a reply token"
    )
    args = parser.parse_args()
    client = None
    try:
        client = PublicCommentsClient(proxy_id=args.proxy_id)
        nodes, info = client.comments(args.post_id)
        result = {
            "mode": "anonymous",
            "comments_on_page": len(nodes),
            "has_next_page": info["has_next_page"],
            "doc_ids": client.doc_ids,
            "validated_operations": ["comments"],
        }
        if args.check_replies:
            parents = [
                node
                for node in nodes
                if (node.get("feedback") or {}).get("id")
                and ((node.get("feedback") or {}).get("expansion_info") or {}).get("expansion_token")
            ]
            if parents:
                feedback = parents[0]["feedback"]
                replies, page = client.replies(feedback["id"], feedback["expansion_info"]["expansion_token"])
                result["validated_operations"].append("replies")
                result["replies_on_page"] = len(replies)
                result["reply_has_next_page"] = page["has_next_page"]
            else:
                result["reply_probe"] = "no_reply_metadata_on_this_page"
        print(json.dumps(result))
        return 0
    except PublicCommentsError as exc:
        print(json.dumps({"status": "failed", "error": type(exc).__name__, "detail": str(exc)}))
        return 1
    except Exception as exc:  # noqa: BLE001 - report type only; proxy/configuration exceptions may contain credentials
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 1
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    raise SystemExit(main())
