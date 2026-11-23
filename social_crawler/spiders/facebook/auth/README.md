# social_crawler/spiders/facebook/auth

Tạo và duy trì phiên Facebook. Luồng chuẩn: `accounts.py` chọn tài khoản -> `bootstrap.py` mở trình duyệt (Playwright),
`triggers.py` bắn thao tác tìm kiếm/bình luận, `request_capture.py` bắt request GraphQL -> lưu token/doc_id vào Redis ->
`graphql_client.py` phát lại bằng HTTP thường. `http_refresh.py` làm mới chỉ qua HTTP; `sdk_fallback.py` (recipe HTTP theo
facebook-gql-scraper) và `operation_fallback.py` (dò Relay bằng V8) là hai đường dự phòng.

## Các file

| File | Việc nó làm |
|---|---|
| `accounts.py` | Chọn dòng platform_accounts mà một lần chạy bootstrap đóng vai - query mới từ D1/DB dev local ở mỗi lời gọi (xem social_crawler/db/accounts.py, services/pool.py), không cache lúc import như biến env FACEBOOK_ACCOUNTS cũ. |
| `bootstrap.py` | Bootstrap một phiên đăng nhập Facebook và bắt một request GraphQL thật để lấy doc_id / fb_dtsg / lsd / __rev... |
| `browser_interaction.py` | Các helper tương tác Playwright chung, dùng chung cho mọi luồng trong package này (đăng nhập, search trigger, comments trigger, ...) - không cái nào biết gì về DOM riêng của Facebook, chỉ biết cách nhìn/thao tác bớt giống trình ... |
| `cookies.py` | Import một phiên Facebook từ cookie lấy được ngoài Playwright (một trình duyệt đã đăng nhập, không tự động) thẳng vào Redis - bỏ qua hẳn luồng đăng nhập tương tác/tự động. |
| `graphql_client.py` | Client HTTP thường (không trình duyệt) gọi lại endpoint GraphQL của Facebook bằng token/doc_id mà bootstrap.py đã cache trong Redis. |
| `http_refresh.py` | Làm mới chỉ qua HTTP và dò Relay ẩn danh, có dự phòng V8 giới hạn. |
| `identity.py` | Facebook dùng danh tính trình duyệt gốc của runtime. |
| `operation_fallback.py` | Dò Relay có giới hạn bằng runtime V8 facebook-gql-scraper đã ghim phiên bản. |
| `public_comments.py` | Bình luận công khai ẩn danh, chuyển thể từ bsho5/fbgql (MIT; licenses/fbgql-MIT.txt). |
| `request_capture.py` | Bắt các request GraphQL bắn ra trong lúc một trigger Playwright chạy, và chọn ra request "initial" / "paginated" trong số đó theo fb_api_req_friendly_name - Facebook đổi tên chúng qua các lần deploy, nên ở đây khớp theo chuỗi ... |
| `sdk_fallback.py` | Dự phòng recipe HTTP dựa trên template XD-MHLOO/facebook-gql-scraper. |
| `triggers.py` | Các luồng Playwright riêng của Facebook: điền và gửi form đăng nhập, và điều khiển các trang search/comments để bắn các request GraphQL mà request_capture.py lắng nghe. |

## Muốn sửa / thêm thì

- Query Facebook không còn khớp: xem `request_capture.py` (khớp theo `fb_api_req_friendly_name`) và [../../../../docs/facebook-sdk-fallback.md](../../../../docs/facebook-sdk-fallback.md).
