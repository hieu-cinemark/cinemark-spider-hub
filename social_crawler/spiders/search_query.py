"""Helper text query tìm kiếm dùng chung cho các spider tìm kiếm facebook/threads - để ngoài
từng spider để cả hai đồng bộ ở điểm này thay vì mỗi bên tự viết lại."""

from __future__ import annotations


def build_search_query(keyword: str) -> str:
    """Thêm "Phim" vào trước một từ khoá tiếng Việt trước khi gửi tới API tìm kiếm của
    Facebook/Threads - KHÔNG thêm trước keyword_match/khử trùng, vốn phải tiếp tục khớp từ
    khoá trơn lưu trong D1 (xem contains_keyword trong posts.py của cinemark-api) - chỗ gọi
    vẫn dùng từ khoá gốc cho việc đó và chỉ dùng hàm này cho lời gọi tìm kiếm gửi đi thật.
    Nhiều tên phim trùng với từ/cụm từ tiếng Việt thông thường có nghĩa đời thường hoàn toàn
    không liên quan (ví dụ "Mẹ Mìn" - cũng là từ dân gian lâu đời chỉ kẻ bắt cóc trẻ em;
    "Loạn Thế" - cũng là cụm từ chung chỉ "thời loạn") - tìm trơn đúng từ/cụm từ đó kéo về
    phần lớn kết quả không liên quan. Thêm "Phim" đẩy cách xếp hạng tìm kiếm của nền tảng
    về phía nghĩa phim.

    Từ khoá tiếng Anh/ASCII được để nguyên (isascii() làm cách đoán "có phải tiếng Việt
    không") - tìm kiểu "Iron Man" không gặp vấn đề trùng này, và "Phim Iron Man" đọc kỳ
    cục. TikTok hoàn toàn không gọi hàm này: tìm kiếm của nó dựa trên hashtag (xem
    TikTokCommentClient/resolve_hashtag), vốn từ chối thẳng khoảng trắng/dấu - không có
    chuỗi query nào để thêm chữ vào trước."""
    stripped = keyword.strip()
    if not stripped or stripped.isascii() or stripped.lower().startswith("phim "):
        return stripped
    return f"Phim {stripped}"
