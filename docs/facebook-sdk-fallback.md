# Facebook: fallback HTTP và V8

Tích hợp ngày 04/11/2026 từ [XD-MHLOO/facebook-gql-scraper](https://github.com/XD-MHLOO/facebook-gql-scraper/tree/3933a99a4eaac2e04754165b0ad7582b1ba452e9), giấy phép MIT. Dependency ghim commit `3933a99a4eaac2e04754165b0ad7582b1ba452e9`, `mini-racer==0.14.1` để tránh tự thay đổi giao thức khi deploy.

## Cách hoạt động

- Giữ công thức đã bắt được nếu còn dùng được. Khi mất công thức search, đọc cookie và User-Agent gốc từ storage state + identity trong Redis. Nếu chưa có state, dùng cookie trong DB kèm `useragent` gốc được mã hóa cùng cookie. Không lấy UA của phiên DB cũ để gắn cho cookie mới đã import. Thiếu metadata hoặc phiên hết hạn thì dừng, không tự đăng nhập.
- GET trang chủ qua proxy của tài khoản, xác nhận `USER_ID` đúng `c_user` và lấy `fb_dtsg`. Dùng các mẫu biến của upstream cho search, comments, Depth1/Depth2 replies, timeline và reels. Không cần mở browser để lấy mẫu ban đầu.
- Đọc Doc ID tĩnh trước. Khi thiếu, chạy runtime module của upstream bằng V8 để giải được module tạo ID động, `.graphql` và `$Parameters`. Khám phá anonymous vẫn không dùng cookie tài khoản. ID khám phá bằng phiên được lưu riêng theo tài khoản, không ghi vào registry công khai.
- Khi Facebook báo Doc ID cũ: thử registry public rồi khám phá qua HTTP của chính tài khoản đang giữ lease; chỉ gửi lại một lần khi có ID khác. Không thay biến để chữa lỗi schema. Không dùng fallback để gửi tiếp sau checkpoint, đăng xuất, HTTP 403/429 hoặc mã hạn chế 368/1675004.
- Consumer tự chuẩn bị query bị thiếu trước job search/comment/reply/feed/reels. Query comment cũ thiếu phần phân trang được bổ sung pagination, giữ phần query gốc. Query có pagination sẵn được giữ nguyên.

Các recipe được tạo mới bằng HTTP gửi bằng `curl_cffi` với target đúng bản Chrome và hệ điều hành trong UA, nên TLS, HTTP/2 và Client Hints khớp UA. Thiếu target tương ứng thì từ chối (`no_matching_tls_transport`), không đổi sang bản khác. Bộ header lấy từ target: GET trang là điều hướng, POST GraphQL là fetch cùng origin, bundle là script cross-site. Không dùng `FacebookContext` của upstream vì transport đó có UA riêng và tắt TLS verification khi dùng proxy. Đường này khác giao thức `requests` của upstream và chưa được thử trên Facebook thật: bằng chứng chỉ từ test với server local.

Mọi GET của fallback qua kiểm tra lease và ngân sách request của cùng tài khoản. GET có cookie đi theo nhịp nghỉ của tài khoản; bundle CDN dùng session riêng không có cookie, chỉ giãn 0,3-1 giây vì 5-10 giây mỗi bundle sẽ hết hạn 75 giây sau khoảng 10 bundle. Trạng thái HTTP của CDN không tính vào tài khoản: bundle lỗi thì bỏ qua. Mỗi tên cookie chỉ gửi một lần, giá trị server vừa set thắng giá trị trong cache. Cookie server xoá được gỡ khỏi mọi recipe và storage state ngay sau response đó; xoá `xs` hoặc `c_user` được coi là đăng xuất: dừng job, tài khoản nghỉ và ghi `dead`. Không đổi proxy, reset IP hoặc chuyển sang tài khoản khác bên trong fallback. Cơ chế chọn proxy ban đầu vẫn do pool hiện có quản lý.

## Nhịp nghỉ theo tài khoản

Mỗi request HTTP của Facebook, kể cả GET lấy token/bundle và POST thử lại, ghi thời điểm hoàn tất. Request tiếp theo chọn khoảng nghỉ ngẫu nhiên tính từ phản hồi đó, thay vì từ lúc bắt đầu gửi. Thời gian xử lý dữ liệu/backoff đã trôi qua được tính vào khoảng nghỉ. Mốc hoàn tất được giữ trong Redis để job sau vẫn tuân theo nhịp nghỉ.

Mỗi UID có dải nghỉ riêng, sinh ổn định bằng hash UID nên không thay đổi khi restart hoặc dùng email thay UID. Dải gốc lấy từ `FACEBOOK_MIN_REQUEST_INTERVAL` (mặc định 5 giây) và `FACEBOOK_REQUEST_JITTER` (mặc định 5 giây); hệ số riêng chỉ làm chậm thêm, không giảm mức sàn. Khi có lỗi mạng/5xx, backoff thích ứng hiện có vẫn tăng khoảng nghỉ. Log `facebook_request_rest_profile` cho biết dải của account, `facebook_request_wait` ghi số giây thực chờ. Mất lease trong lúc nghỉ thì không gửi request tiếp.

## Giới hạn khám phá

- Tối đa 30 bundle/lượt, 75 giây cho luồng khám phá, tối đa 5 MB/response được chấp nhận.
- V8 có heap giới hạn 96 MiB, 2 giây mỗi lần evaluate/thu thập. Timeout hoặc hết bộ nhớ làm lượt khám phá dừng nhưng vẫn lưu các ID đã tìm được. Không có Python callback, token, cookie, API mạng hoặc filesystem trong JS; timer/SharedArrayBuffer của runtime cũng bị tắt.
- Chỉ tải HTTPS từ hostname Facebook đã cho phép; script chỉ từ subdomain `fbcdn.net`. Không theo redirect khi dùng tài khoản. Redirect trên trang HTML không bị coi là hạn chế tài khoản: refresh trả về thất bại, khám phá bỏ qua trang đó; redirect tới checkpoint/đăng xuất vẫn dừng như cũ.
- Không dựng lại phiên từ cookie đã lưu khi tài khoản đang nghỉ, hoặc phiên đã ghi `dead` mà chưa có cookie mới. Tài khoản phải khai báo client `http` và cookie phải qua kiểm tra chất lượng (facebook-accounts.md).
- Cooldown 30 phút theo tài khoản + nhóm operation, kể cả khi khám phá thất bại; registry giữ 24 giờ. Registry public có cooldown riêng. Recipe giữ 7 ngày, token vẫn refresh theo tuổi phiên như trước.

## Cài và kiểm tra

```sh
cd /opt/spider-hub
.venv/bin/python -m pip install -r requirements.txt
```

`FACEBOOK_GQL_FALLBACK_ENABLED=1` mặc định bật. Đặt `0` để tắt việc tạo recipe/khám phá V8 mới; recipe HTTP đã lưu vẫn sử dụng được.

Chuẩn bị riêng công thức cho một tài khoản đã có cookie + User-Agent gốc và proxy trong pool:

```sh
.venv/bin/python -m social_crawler.spiders.facebook.auth.sdk_fallback \
  --account '<UID hoặc email>' --feature search
```

Có thể thay feature bằng `comments`, `replies`, `page_feed`, `page_reels`. Lệnh có giữ lease, tính quota cho HTTP thật, không mở browser, không gửi query crawl hay Kafka. Thiết lập search được thực hiện trước khi chuẩn bị feature khác nếu chưa có phiên search.

Test local dùng V8 thật và template thật, giả lập Facebook/DB/Redis để kiểm tra Doc ID động, timeout, phân trang, cách ly cookie CDN, hạn mức và dừng khi bị hạn chế.

Kiểm chứng thật ngày 04/11/2026 trên account #129/proxy cổng 8159: khởi tạo client từ cookie + UA trong cache kiểm tra riêng, lấy token và Doc ID qua HTTP, tìm một trang bằng client chính của spider-hub. Kết quả **5 request, HTTP 200, trích xuất 4 bài có nội dung**, không mở browser, không gửi Kafka hay ghi dữ liệu crawl vào DB. Dải nghỉ UID này là **5,11–10,6 giây**, log ghi thời gian thực chờ giữa các phản hồi. Không đổi proxy đã gán trong DB; cache kiểm tra được xóa sau lượt chạy, lease và quota thật vẫn áp dụng.

Toàn bộ suite spider-hub: **514 test qua**; script SDK riêng: **24 test qua**. Chưa kiểm chứng bản tích hợp bằng crawl dài; thành công tìm Doc ID cũng không bảo đảm schema/extractor luôn tương thích khi Facebook cập nhật.
