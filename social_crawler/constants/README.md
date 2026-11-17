# social_crawler/constants

Mỗi nền tảng một file hằng số: URL endpoint, mẫu key Redis (`*_REDIS_KEY_TMPL`), mã thoát của tiến trình con, ngưỡng cào.
Giá trị tinh chỉnh được từ dashboard KHÔNG để ở đây mà ở `db/proxy_settings.py`.

## Các file

| File | Việc nó làm |
|---|---|
| `facebook.py` | URL GraphQL, ánh xạ reaction, mẫu key Redis (cache token/doc_id, trạng thái, bình luận) và các ngưỡng của Facebook. |
| `threads.py` | URL GraphQL / text_feed, UA, mẫu key Redis, khoá chống trùng bài (`SEEN_POSTS_KEY`) và ngưỡng của Threads. |
| `tiktok.py` | URL endpoint TikTok, mã thoát (`PROXY_EXHAUSTED_EXIT_CODE` = 75, `SEARCH_PAUSED_EXIT_CODE` = 76), khoá Redis, tham số BFS hashtag. |
