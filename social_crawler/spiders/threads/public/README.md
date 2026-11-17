# social_crawler/spiders/threads/public

`client.py` (GraphQL cho khách) + `channel.py` (hovercard rồi lật tab Threads, mới nhất trước, tới khi gặp bài cũ hơn mốc). Job: `consumer/jobs/threads_channel.py`.

## Các file

| File | Việc nó làm |
|---|---|
| `channel.py` | Đọc bài của một kênh Threads không đăng nhập: hovercard (username -> pk), rồi lật tab Threads 25 bài/trang, mới nhất trước, tới khi gặp bài cũ hơn `since_ts`, hết bài hoặc đủ `max_pages`. |
| `client.py` | Client GraphQL Threads cho khách chưa đăng nhập (curl_cffi giả trình duyệt, không tài khoản). |

## Lưu ý

Threads trả 401 cho một số IP của dải proxy xoay: job thử lại tới 3 lần (IP mới mỗi lần) trước khi cho lối ra đó nghỉ.
