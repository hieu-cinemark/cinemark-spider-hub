"""Client GraphQL Threads cho khách chưa đăng nhập (curl_cffi giả trình duyệt, không tài khoản).

Dựa trên omkarcloud/threads-scraper (MIT, https://github.com/omkarcloud/threads-scraper - threads/fetch.py, queries.py).
Ba điều bắt buộc, thiếu một là Threads trả 200 với "execution error" hoặc 403:
  - mọi cờ relay-provider mà truy vấn khai báo phải có trong `variables` (PROVIDER_VALUES);
  - header `x-logged-out-threads-migrated-request: true`;
  - cookie csrftoken gửi lại trong `x-csrftoken` - token ngẫu nhiên là đủ, không cần mở trang trước; `lsd` cũng ngẫu nhiên.
Mỗi client một phiên curl và một cặp token riêng. Truy vấn là doc_id lưu sẵn: Meta đổi doc_id thì truy vấn trả
"execution error" - lúc đó cập nhật QUERIES theo bản mới của repo trên hoặc bắt lại từ trình duyệt.
"""

from __future__ import annotations

import json
import secrets
import string
from typing import Any

from curl_cffi import requests as cffi_requests

SITE = "https://www.threads.com"
GRAPHQL_URL = SITE + "/graphql/query"
APP_ID = "238260118697367"
ASBD_ID = "359341"
TIMEOUT_SECONDS = 30

# Giá trị cờ relay-provider của khách chưa đăng nhập (threads-scraper queries.PROVIDER_VALUES).
PROVIDER_VALUES: dict[str, bool] = {
    "BarcelonaCanSeeSponsoredContent": False,
    "BarcelonaGenAIRepliesEnabled": False,
    "BarcelonaHasBestOfThreads": False,
    "BarcelonaHasCommunities": True,
    "BarcelonaHasCommunityEmojiUpdateCard": True,
    "BarcelonaHasCommunityEntityCard": False,
    "BarcelonaHasCommunityTopContributors": False,
    "BarcelonaHasDearAlgoConsumption": True,
    "BarcelonaHasDearAlgoWebProduction": False,
    "BarcelonaHasEventBadge": False,
    "BarcelonaHasGameScoreShare": True,
    "BarcelonaHasGhostPostEmojiActivation": False,
    "BarcelonaHasMessaging": False,
    "BarcelonaHasMetaAiContentAttachments": False,
    "BarcelonaHasMusic": True,
    "BarcelonaHasNewspaperLinkStyle": False,
    "BarcelonaHasPodcastTranscriptConsumption": True,
    "BarcelonaHasPodcastV2Consumption": True,
    "BarcelonaHasPrivateRepliesDeprecation": False,
    "BarcelonaHasProfileSelfReplyContext": False,
    "BarcelonaHasPublicViewCountCard": True,
    "BarcelonaHasScorecardCommunity": False,
    "BarcelonaHasSportTeamAllegianceCard": False,
    "BarcelonaHasViewerReplied": True,
    "BarcelonaHasWebFavicons": False,
    "BarcelonaIsCrawler": False,
    "BarcelonaIsInternalUser": False,
    "BarcelonaIsLoggedIn": False,
    "BarcelonaIsSearchDiscoveryEnabled": False,
    "BarcelonaMessagesHasLiveChatMessaging": False,
    "BarcelonaMessagingHasMetaAIBot": False,
    "BarcelonaOptionalCookiesEnabled": True,
    "BarcelonaShouldFetchPostAuthorFullName": False,
    "BarcelonaShouldFulfillLightboxQuery": True,
    "BarcelonaShouldShowFediverseM075Features": False,
    "BarcelonaShouldShowFediverseM1Features": False,
}

QUERIES: dict[str, dict[str, Any]] = {
    # Hồ sơ theo username -> pk (id số) mà truy vấn tab cần.
    "BarcelonaUsernameHovercardImplDirectQuery": {
        "doc_id": "28097126603320603",
        "root": "xdt_text_app_user_by_username",
        "providers": [
            "BarcelonaIsInternalUser",
            "BarcelonaIsLoggedIn",
            "BarcelonaHasMessaging",
            "BarcelonaMessagesHasLiveChatMessaging",
            "BarcelonaShouldShowFediverseM1Features",
            "BarcelonaHasEventBadge",
        ],
    },
    # Tab Threads của hồ sơ: 25 bài/trang (cố định phía máy chủ), con trỏ end_cursor, mới nhất trước.
    "BarcelonaProfileThreadsTabRefetchableDirectQuery": {
        "doc_id": "39532365406377298",
        "root": "xdt_api__v1__text_feed__user_id__profile__connection",
        "providers": [
            "BarcelonaIsLoggedIn",
            "BarcelonaHasProfileSelfReplyContext",
            "BarcelonaHasDearAlgoConsumption",
            "BarcelonaHasMetaAiContentAttachments",
            "BarcelonaShouldFetchPostAuthorFullName",
            "BarcelonaMessagesHasLiveChatMessaging",
            "BarcelonaHasEventBadge",
            "BarcelonaHasBestOfThreads",
            "BarcelonaMessagingHasMetaAIBot",
            "BarcelonaGenAIRepliesEnabled",
            "BarcelonaIsSearchDiscoveryEnabled",
            "BarcelonaHasCommunities",
            "BarcelonaHasGameScoreShare",
            "BarcelonaHasPublicViewCountCard",
            "BarcelonaHasCommunityEmojiUpdateCard",
            "BarcelonaHasCommunityEntityCard",
            "BarcelonaHasScorecardCommunity",
            "BarcelonaHasSportTeamAllegianceCard",
            "BarcelonaHasMusic",
            "BarcelonaHasNewspaperLinkStyle",
            "BarcelonaHasMessaging",
            "BarcelonaHasPodcastV2Consumption",
            "BarcelonaHasPodcastTranscriptConsumption",
            "BarcelonaShouldFulfillLightboxQuery",
            "BarcelonaHasViewerReplied",
            "BarcelonaHasPrivateRepliesDeprecation",
            "BarcelonaHasGhostPostEmojiActivation",
            "BarcelonaOptionalCookiesEnabled",
            "BarcelonaHasDearAlgoWebProduction",
            "BarcelonaCanSeeSponsoredContent",
            "BarcelonaHasWebFavicons",
            "BarcelonaIsCrawler",
            "BarcelonaHasCommunityTopContributors",
            "BarcelonaShouldShowFediverseM075Features",
            "BarcelonaIsInternalUser",
        ],
    },
}


class ThreadsPublicError(RuntimeError):
    """Lỗi mạng / 5xx / "execution error" - thử lại được (execution error kéo dài = doc_id đã đổi)."""


class ThreadsPublicBlocked(ThreadsPublicError):
    """401/403/429 hoặc HTML thay vì JSON: IP này đang bị chặn - đổi proxy/IP."""


def _token(n: int) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(secrets.choice(chars) for _ in range(n))


def variables_for(name: str, **variables: Any) -> dict[str, Any]:
    """Biến của người gọi cộng mọi cờ relay-provider mà truy vấn khai báo."""
    out = dict(variables)
    for provider in QUERIES[name]["providers"]:
        out[f"__relay_internal__pv__{provider}relayprovider"] = bool(PROVIDER_VALUES.get(provider, False))
    return out


class ThreadsPublicClient:
    """Một phiên khách: proxy tuỳ chọn (dạng http://user:pass@host:port), token ngẫu nhiên riêng."""

    def __init__(self, proxy: str | None = None) -> None:
        self.session = cffi_requests.Session(impersonate="chrome")
        if proxy:
            self.session.proxies = {"http": proxy, "https": proxy}
        self.csrf = _token(32)
        self.lsd = "AV" + _token(9)
        self.session.cookies.set("csrftoken", self.csrf, domain=".threads.com")

    def close(self) -> None:
        self.session.close()

    def graphql(self, name: str, **variables: Any) -> dict[str, Any]:
        """Chạy một truy vấn lưu sẵn -> object `data`. Thực thể không tồn tại: data với root null (không lỗi)."""
        query = QUERIES[name]
        body = {
            "lsd": self.lsd,
            "doc_id": query["doc_id"],
            "variables": json.dumps(variables_for(name, **variables), separators=(",", ":")),
            "fb_api_req_friendly_name": name,
            "fb_api_caller_class": "RelayModern",
            "server_timestamps": "true",
            "__a": "1",
            "__comet_req": "29",
            "__user": "0",
            "av": "0",
        }
        headers = {
            "accept": "*/*",
            "accept-language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
            "content-type": "application/x-www-form-urlencoded",
            "origin": SITE,
            "referer": SITE + "/",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "x-asbd-id": ASBD_ID,
            "x-csrftoken": self.csrf,
            "x-fb-friendly-name": name,
            "x-fb-lsd": self.lsd,
            "x-ig-app-id": APP_ID,
            "x-logged-out-threads-migrated-request": "true",
            "x-root-field-name": query["root"],
        }
        try:
            resp = self.session.post(GRAPHQL_URL, data=body, headers=headers, timeout=TIMEOUT_SECONDS)
        except Exception as exc:
            raise ThreadsPublicError(f"{name}: {type(exc).__name__}: {str(exc)[:200]}") from exc
        text = resp.text or ""
        if resp.status_code in (401, 403, 429):
            raise ThreadsPublicBlocked(f"HTTP {resp.status_code} on {name}")
        if not text.lstrip().startswith("{"):
            raise ThreadsPublicBlocked(f"{name} returned a non-JSON body (HTTP {resp.status_code})")
        if resp.status_code != 200:
            raise ThreadsPublicError(f"HTTP {resp.status_code} on {name}")
        payload = json.loads(text)
        data = payload.get("data")
        if data is None:
            raise ThreadsPublicError(f"{name} returned no data: {str(payload.get('errors'))[:200]}")
        return data

    def user_by_username(self, username: str) -> dict[str, Any] | None:
        """{pk, username, full_name, follower_count, ...} hoặc None khi không có người dùng này."""
        data = self.graphql("BarcelonaUsernameHovercardImplDirectQuery", username=username.lstrip("@"))
        user = data.get("user") or data.get(QUERIES["BarcelonaUsernameHovercardImplDirectQuery"]["root"])
        return user if isinstance(user, dict) and (user.get("pk") or user.get("id")) else None

    def profile_threads_page(self, user_id: str, cursor: str | None = None) -> tuple[dict[str, Any], str | None]:
        """Một trang tab Threads: (data thô để extract_response đọc, con trỏ trang sau hoặc None khi hết)."""
        data = self.graphql(
            "BarcelonaProfileThreadsTabRefetchableDirectQuery",
            userID=str(user_id),
            after=cursor,
            before=None,
            first=25,
            last=None,
            allow_page_info_for_lox_user=True,
        )
        connection = (
            data.get("mediaData") or data.get(QUERIES["BarcelonaProfileThreadsTabRefetchableDirectQuery"]["root"]) or {}
        )
        info = connection.get("page_info") or {}
        cursor = info.get("end_cursor") if info.get("has_next_page") else None
        return data, cursor or None
