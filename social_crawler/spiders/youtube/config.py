"""YouTube crawler constants (endpoints, client identity, URLs)."""

import os

ENDPOINT_BROWSE = "browse"
ENDPOINT_SEARCH = "search"
ENDPOINT_PLAYER = "player"
ENDPOINT_NEXT = "next"
SEARCH_FILTER_LIVE = "EgJAAQ%3D%3D"
SORT_RELEVANCE = None
SORT_UPLOAD_DATE = "CAISAhAB"
SORT_VIEW_COUNT = "CAMSAhAB"
SORT_RATING = "CAESAhAB"
CHANNEL_TAB_VIDEOS = "EgZ2aWRlb3M"
CLIENT_NAME = "WEB"
CLIENT_VERSION = "2.20260603.05.00"
CLIENT_HL = "vi"
CLIENT_GL = "VN"
# Khoá InnerTube công khai của web youtube.com - đọc từ trang chủ lúc chạy (client._scrape_homepage), không ghi cứng trong
# repo. Ghi đè bằng YOUTUBE_INNERTUBE_KEY nếu cần; để trống mà đọc trang chủ lỗi thì gọi API không kèm key (vẫn chạy).
INNERTUBE_API_KEY = os.getenv("YOUTUBE_INNERTUBE_KEY", "")
DEFAULT_TIMEOUT = 15
YOUTUBE_KEY_TTL = 86400
YOUTUBE_BASE_URL = "https://www.youtube.com"
YOUTUBE_API_BASE = "https://www.youtube.com/youtubei/v1"

# Topic maps for agent tools and /api/videos/by-topic
CHANNEL_TOPICS: dict[str, str] = {"music": "UC-9-kyTW8ZkZNDHQJ6FgpwQ"}
SEARCH_TOPICS: dict[str, tuple[str, str]] = {
    "gaming": ("gaming highlights", "view_count"),
    "news": ("tin tức việt nam hôm nay", "upload_date"),
    "sports": ("thể thao việt nam", "view_count"),
    "tech": ("review công nghệ", "view_count"),
    "beauty": ("beauty skincare review", "view_count"),
    "food": ("ẩm thực việt nam", "view_count"),
    "travel": ("du lịch việt nam", "view_count"),
}
ALL_TOPICS = list(CHANNEL_TOPICS.keys()) + list(SEARCH_TOPICS.keys())

HTTP_MAX_CONNECTIONS = 20
HTTP_MAX_KEEPALIVE = 10
