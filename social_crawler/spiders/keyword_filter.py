"""Lọc bài lạc đề ngay trong spider tìm kiếm Facebook/Threads (2026-10-15).

Tìm kiếm của Facebook/Threads trả nhiều bài KHÔNG chứa từ khoá: tên phim trùng cụm từ thông dụng ("Ngày Con Còn Mẹ",
"Mật Mã Đông Dương" kéo về truyện mạng, phim Trung Quốc) và Threads trả bài ngẫu nhiên toàn cầu khi truy vấn ít kết quả.
Đo 01-15/10/2026: Facebook 74% bài liên quan, Threads 24%; trong số bài liên quan, chỉ 102/26.518 (FB) và 157/5.977
(Threads) không chứa từ khoá (Kira chấm liên quan) - nên bỏ bài không chứa từ khoá ngay ở đây mất < 1% dữ liệu tốt
nhưng cắt phần lớn rác trước khi tốn Kafka/D1 (quota ghi) và thời gian của tài khoản/proxy.

So khớp GIỐNG HỆT keyword_match của ingest (contains_keyword trong cinemark-api app/repositories/d1/posts.py): chữ
thường, bỏ dấu, đ -> d, bỏ khoảng trắng; từ khoá nối bằng "+" thì mọi phần đều phải có. Bài bị bỏ ở đây đằng nào cũng
sẽ là keyword_match = 0 (ẩn) khi ingest.

OffTopicGuard: dừng từ khoá sớm khi nhiều trang liền gần như toàn bài lạc đề - trang sau của tìm kiếm còn tệ hơn.
"""

from __future__ import annotations

import json
import re
import unicodedata

# Trang có ít nhất chừng này bài mới xét tỉ lệ (trang quá ít bài thì tỉ lệ không nói lên gì).
MIN_PAGE_POSTS = 5
# Tỉ lệ bài chứa từ khoá dưới mức này là trang "lạc đề".
MIN_MATCH_RATIO = 0.10
# Số trang lạc đề liên tiếp thì dừng từ khoá.
OFF_TOPIC_PAGE_STREAK = 2


def fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text)
    without_marks = "".join(c for c in decomposed if not unicodedata.combining(c))
    without_dd = without_marks.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"\s+", "", without_dd.lower())


def _alnum(text: str) -> str:
    return re.sub(r"[\W_]+", "", fold(text))


def keyword_parts(keyword: str) -> list[str]:
    if "+" in keyword:
        return [part.strip() for part in keyword.split("+") if part.strip()]
    trimmed = keyword.strip()
    return [trimmed] if trimmed else []


def mentions_keyword(text: str | None, keyword: str | None) -> bool:
    if not text or not keyword:
        return False
    haystack = fold(text)
    parts = keyword_parts(keyword)
    return bool(parts) and all((needle := fold(part)) and needle in haystack for part in parts)


class OffTopicGuard:
    """Đếm bài khớp / lạc đề của một lượt tìm kiếm và báo khi nên dừng từ khoá."""

    def __init__(self, keyword: str, match_terms: str | list[str] | None = None) -> None:
        self.keyword = keyword
        # Tên phim (JSON list từ crawl request, hoặc list): bài nhắc một trong các tên này cũng được giữ.
        if isinstance(match_terms, str):
            try:
                parsed = json.loads(match_terms)
            except ValueError:
                parsed = [match_terms]
            match_terms = parsed if isinstance(parsed, list) else [str(parsed)]
        # So tên phim bỏ qua dấu câu ("SCOTTY: GIẢI CỨU..." khớp "Scotty Giải Cứu..."); từ khoá vẫn so y như ingest.
        self.titles = [loose for term in match_terms or [] if (loose := _alnum(str(term)))]
        self.matched = 0
        self.dropped = 0
        self._streak = 0

    def keep(self, text: str | None) -> bool:
        ok = mentions_keyword(text, self.keyword) or (bool(text) and any(title in _alnum(text) for title in self.titles))
        if ok:
            self.matched += 1
        else:
            self.dropped += 1
        return ok

    def new_search(self) -> None:
        """Bắt đầu một lượt tìm khác (vd. tab Video): chuỗi trang lạc đề của lượt trước không tính sang."""
        self._streak = 0

    def end_page(self, page_matched: int, page_total: int) -> bool:
        """Gọi sau mỗi trang với số bài khớp / tổng số bài của trang. True = nên dừng từ khoá."""
        if page_total >= MIN_PAGE_POSTS and page_matched / page_total < MIN_MATCH_RATIO:
            self._streak += 1
        elif page_total > 0:
            self._streak = 0
        return self._streak >= OFF_TOPIC_PAGE_STREAK


# Bộ đếm theo từ khoá cho cinemark-api (app/services/keyword_yield.py): bài khớp / bị bỏ vì lạc đề, cộng dồn, hết hạn sau
# YIELD_TTL_SECONDS kể từ lần cào cuối - để biết từ khoá nào toàn ra rác khi bài rác không còn được lưu vào D1.
YIELD_KEY_TMPL = "keyword_yield:{keyword_id}:{kind}"
YIELD_TTL_SECONDS = 14 * 24 * 3600


def save_yield(cache, keyword_id: str | None, guard: OffTopicGuard) -> None:
    if cache is None or not keyword_id or not (guard.matched or guard.dropped):
        return
    for kind, amount in (("matched", guard.matched), ("dropped", guard.dropped)):
        key = YIELD_KEY_TMPL.format(keyword_id=keyword_id, kind=kind)
        if amount:
            cache.incr(key, amount)
        cache.expire(key, YIELD_TTL_SECONDS)


# Tên phim ngắn hơn chừng này (sau khi bỏ dấu/khoảng trắng/dấu câu) không so chuỗi con trong bài kênh - quá dễ trùng chữ
# thường. Cùng ngưỡng với match_movie của omar_channels (cinemark-api).
MIN_CHANNEL_TITLE_CHARS = 6
_HASHTAG_RE = re.compile(r"#(\w+)", re.UNICODE)


def _parse_terms(terms: str | list[str] | None) -> list[str]:
    """JSON list (tham số -a của scrapy), list, hoặc một chuỗi trơn."""
    if isinstance(terms, str):
        try:
            parsed = json.loads(terms)
        except ValueError:
            parsed = [terms]
        terms = parsed if isinstance(parsed, list) else [str(parsed)]
    return [str(term) for term in terms or [] if str(term).strip()]


class ChannelMovieMatcher:
    """Bài của một kênh (spider threads_channel_posts) có nói về phim đang nghiệm thu không - cùng ý với match_movie của
    omar_channels phía cinemark-api: một hashtag của bài TRÙNG hẳn một hashtag của phim, hoặc bài chứa tên phim (bỏ dấu,
    khoảng trắng, dấu câu); thêm: bài chứa từ khoá (so như ingest). Kênh đăng đủ thứ, nên không giữ bài nào không nhắc
    phim - ingest cũng sẽ bỏ chúng (keyword_absent).

    Không có từ khoá/tên/hashtag nào (request thiếu thông tin) thì giữ mọi bài, để ingest tự lọc."""

    def __init__(
        self,
        keyword: str | None = None,
        titles: str | list[str] | None = None,
        hashtags: str | list[str] | None = None,
    ) -> None:
        self.keyword = (keyword or "").strip() or None
        self.titles = [loose for term in _parse_terms(titles) if len(loose := _alnum(term)) >= MIN_CHANNEL_TITLE_CHARS]
        self.tags = {tag for term in _parse_terms(hashtags) if (tag := _alnum(term.lstrip("#")))}
        self.matched = 0
        self.dropped = 0

    @property
    def unrestricted(self) -> bool:
        return not (self.keyword or self.titles or self.tags)

    def keep(self, text: str | None, topic: str | None = None) -> bool:
        """topic: chủ đề (tag_header) của bài Threads - hiện như một hashtag trên giao diện, không nằm trong caption."""
        if self.unrestricted:
            ok = True
        else:
            post_tags = {_alnum(tag) for tag in _HASHTAG_RE.findall(text or "")}
            if topic:
                post_tags.add(_alnum(topic.lstrip("#")))
            loose = _alnum(text) if text else ""
            ok = (
                bool(self.tags & post_tags)
                or (bool(loose) and any(title in loose for title in self.titles))
                or mentions_keyword(text, self.keyword)
            )
        if ok:
            self.matched += 1
        else:
            self.dropped += 1
        return ok
