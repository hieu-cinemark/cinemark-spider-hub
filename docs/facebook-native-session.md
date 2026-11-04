# Facebook: native browser identity and request controls

## Hành vi mới

Facebook dùng UA/platform do chính Chromium đang chạy trả về, không đặt UA/Client Hints qua CDP, không giả macOS/DPR/screen. Locale và timezone cũng dùng mặc định của runtime. Threads và các client cũ tiếp tục dùng đường cấu hình hiện có.

Trước Restore, một context trống chỉ mở `about:blank` để đọc UA; chưa nạp cookie hoặc truy cập Facebook. Nếu UA gốc không có, khác hệ điều hành, phiên bản hoặc chế độ headed/headless, Restore dừng với lỗi thiết lập. Không tự nâng phiên bản UA, chọn profile thay thế hay đánh dấu tài khoản chết. Một lần đăng nhập mới có chủ đích được dùng runtime native hiện tại; chỉ sau khi thành công mới lưu metadata mới và vô hiệu hóa query của phiên cũ.

HTTP giữ nguyên header đã quan sát từ browser, không chạy `normalize_cached_fingerprint` cho Facebook. `default_headers=False` tránh để curl_cffi thêm bộ header mặc định khác OS. Target curl được chọn theo phiên bản đã ghi và hỗ trợ có sẵn; đây **không phải bằng chứng** TLS/HTTP2 trùng hoàn toàn với Chromium hoặc Facebook không nhận diện tự động hóa. Cache có OS khác runtime hoặc header mâu thuẫn bị từ chối trước request.

Import cookie mới xóa công thức query và metadata thiết bị của phiên cũ. UA đi kèm được giữ nguyên; không có UA gốc thì cần thiết lập lại, không mượn danh tính browser của cookie cũ.

## Phiên đã có trước thay đổi

Không tự chuyển các cookie từ Mac/local sang hồ sơ Linux/VPS. Khi gặp lỗi identity, cần đăng nhập/thiết lập lại có chủ đích trên máy và chế độ browser sẽ dùng. Ví dụ sau khi đã chọn tài khoản cần thiết lập:

```sh
.venv/bin/python -m social_crawler.spiders.facebook.auth.bootstrap \
  --manual --show-browser --account '<account-key>' --query '<query>'
```

`--show-browser` cần màn hình hoặc Xvfb. Phiên headed không tự động được đổi sang headless khi Restore. Việc thiết lập/query còn thiếu vẫn cần thao tác riêng; consumer không tự mở browser để khắc phục.

## Request và kiểm tra cookie

- Ngân sách và mốc throttle Facebook không đọc/ghi được vào Redis: dừng trước request. Trần giờ vẫn có hiệu lực khi tắt trần ngày.
- `__req` được cấp nguyên tử trong Redis theo account và capture/context, tiếp tục qua các job và tăng cho mỗi lần gửi thật, kể cả retry. Capture mới có ID nội bộ riêng; HTTP refresh giữ nguyên ID đó. Không thay `__s`/các trường chưa xác minh. Định dạng counter chưa hỗ trợ báo lỗi thay vì reset về 1; đây là kiểm soát replay của ứng dụng, không phải khẳng định về quy tắc phát hiện của Facebook.
- cinemark-api mặc định tắt lịch check cookie cho cấu hình mới. Cấu hình cũ đã bật vẫn chỉ thực hiện phép đọc thụ động trên spider-hub.

## Kiểm chứng

Unit test dùng dữ liệu giả cho identity/Restore, import cookie, invalidation query, sequence qua nhiều client/retry, lỗi Redis và nhánh trần giờ. Browser smoke chỉ mở `about:blank` local, không tài khoản/cookie Facebook. Chưa kiểm chứng phiên thực tế trên VPS hoặc mức giảm checkpoint; test header loopback không phải chứng nhận anti-bot.
