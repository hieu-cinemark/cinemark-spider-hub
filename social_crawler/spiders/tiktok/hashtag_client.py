"""Client TikTok cho hashtag: tìm video theo hashtag."""

from __future__ import annotations

from typing import Any

from social_crawler.constants.tiktok import (
    HASHTAG_DETAIL_URL,
    HASHTAG_ITEM_LIST_URL,
)
from social_crawler.core.logger import get_logger
from social_crawler.spiders.tiktok.client import TikTokClient

logger = get_logger(__name__)


class TikTokHashtagClient(TikTokClient):
    def resolve_hashtag(self, name: str) -> str | None:
        """id TikTok dạng số của một hashtag, cho trước tên (không có '#' ở đầu, không khoảng trắng -
        ví dụ "holinhtrangsi"). None nếu TikTok không có hashtag đó. id này là thứ `challenge_id`
        của search_hashtag() cần - nó không đổi, nên chỗ gọi có thể tra một lần rồi dùng lại cho
        mọi lời gọi search_hashtag()/phân trang sau đó."""
        name = name.lstrip("#").strip()
        if not name.isascii() or " " in name:
            raise ValueError(
                f"{name!r} isn't a TikTok hashtag slug - pass the actual tag "
                "(no spaces/diacritics, e.g. 'holinhtrangsi'), not a movie "
                "title or display keyword. curl_cffi can't put non-ASCII "
                "text in a header, and TikTok's real hashtag ids look "
                "nothing like a Vietnamese title anyway."
            )
        data = self._request(HASHTAG_DETAIL_URL, {"challengeName": name}, referer=f"https://www.tiktok.com/tag/{name}")
        return (data.get("challengeInfo") or {}).get("challenge", {}).get("id")

    def search_hashtag(
        self,
        challenge_id: str,
        cursor: int = 0,
        count: int = 30,
        hashtag: str = "",
        *,
        count_block: bool = True,
    ) -> dict[str, Any]:
        """Lấy một trang video của một hashtag (trang đầu khi cursor là 0). `challenge_id` là id
        hashtag dạng số của TikTok - không phải chính tên hashtag (xem resolve_hashtag). Referer
        phải là URL công khai `/tag/<slug>`, khớp với challenge/detail - `/tag/<id>` là thứ mà
        item_list đã đăng nhập từ chối bằng 200 rỗng."""
        slug = hashtag.lstrip("#").strip() or challenge_id
        return self._request(
            HASHTAG_ITEM_LIST_URL,
            {"challengeID": challenge_id, "count": str(count), "cursor": str(cursor)},
            referer=f"https://www.tiktok.com/tag/{slug}",
            count_block=count_block,
        )
