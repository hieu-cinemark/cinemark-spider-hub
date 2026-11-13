from social_crawler.spiders.youtube.config import SORT_RATING, SORT_RELEVANCE, SORT_UPLOAD_DATE, SORT_VIEW_COUNT

SORT_OPTIONS = {
    "relevance": SORT_RELEVANCE,
    "upload_date": SORT_UPLOAD_DATE,
    "view_count": SORT_VIEW_COUNT,
    "rating": SORT_RATING,
}

# Bộ lọc của trang tìm kiếm (tham số sp trên youtube.com). Mã sắp xếp "upload_date" ở trên (CAISAhAB) không còn tác dụng
# từ 2026 - YouTube trả cùng thứ tự như relevance; lấy video mới bằng bộ lọc ngày tải lên thay vào.
SEARCH_FILTERS = {
    "relevance": None,
    "today": "EgIIAg%3D%3D",
    "week": "EgIIAw%3D%3D",
    "month": "EgIIBA%3D%3D",
    "year": "EgIIBQ%3D%3D",
    "shorts": "EgIQCQ%3D%3D",
    "videos": "EgIQAQ%3D%3D",
    # Thời lượng (dưới 4 phút / 4-20 phút / trên 20 phút) và sắp theo lượt xem: mỗi cách cắt trả một tập kết quả khác.
    "under4": "EgIYAQ%3D%3D",
    "4to20": "EgIYAw%3D%3D",
    "over20": "EgIYAg%3D%3D",
    "view_count": "CAM%3D",
    "newest": "CAI%3D",
}
