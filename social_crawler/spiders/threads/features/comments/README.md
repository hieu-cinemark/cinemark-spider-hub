# social_crawler/spiders/threads/features/comments

GET `/api/v1/text_feed/<id>/replies/` qua curl_cffi bằng cookie phiên đã có; `extract.py` trích reply.

## Các file

| File | Việc nó làm |
|---|---|
| `comments.py` | Spider reply Threads không bao giờ mở trình duyệt: GET /api/v1/text_feed/<post_id>/replies/ qua curl_cffi bằng cùng session cookie mà search vốn bootstrap (ds_user_id/sessionid/csrftoken). |
| `extract.py` | Trích reply Threads từ response GET /api/v1/text_feed/<id>/replies/ (extract_replies_from_text_feed) - thứ comments.py crawl. |
