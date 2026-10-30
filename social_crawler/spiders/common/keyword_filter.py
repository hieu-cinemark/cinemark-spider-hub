"""Lọc bài lạc đề ngay trong spider tìm kiếm Facebook/Threads (2026-10-15).

Tìm kiếm của Facebook/Threads trả nhiều bài KHÔNG chứa từ khoá: tên phim trùng cụm từ thông dụng ("Ngày Con Còn Mẹ",
"Mật Mã Đông Dương" kéo về truyện mạng, phim Trung Quốc) và Threads trả bài ngẫu nhiên toàn cầu khi truy vấn ít kết quả.
Đo 01-15/10/2026: Facebook 74% bài liên quan, Threads 24%; trong số bài liên quan, chỉ 102/26.518 (FB) và 157/5.977
(Threads) không chứa từ khoá (AI chấm liên quan) - nên bỏ bài không chứa từ khoá ngay ở đây mất < 1% dữ liệu tốt
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
