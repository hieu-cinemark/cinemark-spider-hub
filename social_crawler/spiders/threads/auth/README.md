# social_crawler/spiders/threads/auth

Giống `facebook/auth` nhưng cho threads.com; `open_browser.py` mở trình duyệt có giao diện để người vận hành xử lý checkpoint.

## Các file

| File | Việc nó làm |
|---|---|
| `accounts.py` | Chọn dòng platform_accounts (platform='threads') mà một lần chạy bootstrap đóng vai - threads.com đăng nhập qua hệ thống tài khoản của Instagram, nên phần này giống social_crawler.spiders.facebook.auth.accounts từng trường một ... |
| `bootstrap.py` | Bootstrap một phiên đăng nhập threads.com và bắt một request GraphQL thật để lấy doc_id / fb_dtsg / lsd / __rev... |
| `cookies.py` | Import một phiên threads.com từ cookie lấy được ngoài Playwright (một trình duyệt đã đăng nhập, không tự động) thẳng vào Redis - giống social_crawler.spiders.facebook.auth.cookies, nhưng threads.com cần domain cookie và bộ cookie ... |
| `graphql_client.py` | Client HTTP thường (không trình duyệt) gọi lại endpoint GraphQL của Threads bằng token/doc_id mà bootstrap.py đã cache trong Redis. |
| `open_browser.py` | Mở một trình duyệt có giao diện đã đăng nhập vào session sẵn có của tài khoản Threads đang được xoay tới, để người dùng xem / xử lý một thử thách checkpoint trong trình duyệt mà Meta đưa ra giữa phiên (xem body response ... |
| `request_capture.py` | Bắt các request GraphQL bắn ra trong lúc một trigger Playwright chạy - giống social_crawler.spiders.facebook.auth.request_capture.capture_graphql_requests, nhưng khớp URL endpoint thật của threads.com thay vì của Facebook. |
| `triggers.py` | Các luồng Playwright riêng của Threads: điều khiển trang tìm kiếm để bắn các request GraphQL mà request_capture.py lắng nghe. |
