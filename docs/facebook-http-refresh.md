# Facebook: làm mới phiên qua HTTP

Thay đổi ngày 03/11/2026, đối chiếu [facebook-gql-scraper](https://github.com/XD-MHLOO/facebook-gql-scraper/tree/c475eb4b75d18ea099aa63521f673d94a06195c5).

- Consumer không tự mở browser để thay phiên Facebook sau 6 giờ hoặc khi thiếu query search/comment/reply/feed/reels. Thiếu thiết lập ban đầu thì job báo lỗi; đăng nhập/Restore/bootstrap bằng tay vẫn là thao tác riêng.
- Công thức query giữ 7 ngày. Sau 6 giờ, GET trang chủ bằng chính curl_cffi session, proxy, hồ sơ TLS/UA và cookie của tài khoản. Chỉ lưu token nếu HTML xác nhận đúng USER_ID. GET có tính ngân sách, throttle và lease. Thất bại nghỉ thử refresh 30 phút, không chuyển tài khoản hay mở browser.
- Header `c_user=deleted` hoặc `set-login: logged-out`: xóa phiên, ghi dead/cooldown, chặn mọi request tiếp theo của client. POST không tự theo redirect. Thiếu request capture và `CometLogoutHandlerQuery` không còn là bằng chứng phiên chết.
- `doc_id` không tồn tại: thử khám phá công khai và replay tối đa một lần nếu tìm được ID khác. Sai biến hoặc cấu trúc reply: báo lỗi schema; lỗi GraphQL khác không được tính thành trang rỗng thành công.
- Reply dùng Depth1/Depth2, ID `feedback:<post>_<comment>`, expansionToken và cursor đúng từng cấp. Token mở rộng chỉ dùng trong bộ nhớ, không gửi Kafka. Vẫn giữ cờ `include_replies` như trước; chưa bật crawl thật để kiểm chứng tài khoản.
- cinemark-api hiển thị tuổi token dựa trên `http_refreshed_at`/`captured_at`, không coi TTL lưu công thức 7 ngày là phiên còn mới.

## Một nguồn trạng thái phiên

Cookie của `storage_state` (trình duyệt) và công thức query (HTTP) được ghi cùng nhau qua `cookies.save_http_cookies` / `cookies.save_browser_state`, trong lúc giữ lease của tài khoản. Client đọc lại công thức ngay sau khi nhận lease. State không có `c_user` hoặc mang danh tính khác thì không ghi đè cookie của phía còn lại.

Cookie mới từ các POST crawl thường chưa được lưu xuống Redis; chỉ HTTP refresh và lần khôi phục token mới lưu.

## Tín hiệu hạn chế

Cho nghỉ `META_WARNING_REST_HOURS` (12 giờ), nhả khỏi làn/con trỏ active, giữ phiên và không ghi `dead`, khi gặp: HTTP 403/3xx trên GraphQL, 429 sau khi hết lượt thử lại, mã lỗi `368`/`1675004`, hoặc câu cảnh báo nằm trong trường lỗi có cấu trúc. Không quét `data` hay HTML. Header `Location` tới `/checkpoint/` được xử lý như checkpoint (tắt tài khoản).

Mã và câu chữ **chưa được đối chiếu với phản hồi thật trên hệ thống này**. Lỗi GraphQL khác chỉ làm hỏng job và ghi log `graphql_error_unclassified` (mã + câu thông báo) để bổ sung danh sách từ dữ liệu thật.

## Khám phá không đăng nhập

```sh
cd /opt/spider-hub
.venv/bin/python -m social_crawler.spiders.facebook.auth.http_refresh
```

Mặc định dùng proxy #15 (`FACEBOOK_PUBLIC_DISCOVERY_PROXY_ID`, đổi từ #10 ngày 03/11/2026 vì #10 đã gán cho tài khoản). Proxy phải bật và không được gán cho bất kỳ tài khoản nào, kể cả tài khoản tắt. Không gọi link xoay IP. Client riêng, không nạp cookie/state tài khoản; xóa guest cookie trước mỗi GET, chỉ đọc Facebook/CDN Facebook, không thực thi JS. Tối đa 30 bundle, thời hạn 75 giây, cooldown dùng chung 30 phút. Các operation tìm được lưu Redis riêng, không ghi đè template biến.

Phép thử local ngày 03/11/2026 qua #10 tìm được **12 operation công khai, không có search/comment/Depth1/Depth2**; một trang trả 404. Chưa chứng minh có thể làm mới các ID cần dùng hoàn toàn không đăng nhập. Không dùng ID đoán, không chuyển sang tài khoản để tìm khi phép thử này thất bại. Cách đọc bundle hiện hỗ trợ module Relay khai báo ID tĩnh; không chạy runtime V8 như repo tham khảo.

Cache cũ còn sống được giữ lại khi consumer đọc. Nếu cache đã hết hạn trước rollout, cần thiết lập phiên/query bằng tay; HTTP refresh không thể tự tạo lại một template đã mất. Chưa thử HTTP refresh/reply trên tài khoản thật, chưa có bằng chứng giảm checkpoint. Group công khai và reaction theo loại để sau.
