# social_crawler/spiders/tiktok/features/comments

Phân trang `/api/comment/list/`; reply bị chặn chỉ bỏ riêng bình luận đó (thử danh tính mới một lần), không làm lại cả video.

## Các file

| File | Việc nó làm |
|---|---|
| `comments.py` | Spider comment TikTok qua curl_cffi — cùng kiến trúc với tiktok_hashtag_search: tạo một danh tính khách synthetic mới, ký từng request ở local (X-Gnarly + X-Dynosaur), phân trang /api/comment/list/. |
| `extract.py` | Trích comment / reply TikTok từ JSON /api/comment/list/ và /api/comment/list/reply/ (TikTokCommentClient). |
