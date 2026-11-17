# social_crawler/consumer

Tiến trình chạy lâu của spider-hub. Mỗi nền tảng một vòng lặp đọc Kafka `crawl_requests`; thêm các worker đọc hàng đợi Redis
(làn ưu tiên, hàng đợi rảnh, trang công khai, kênh Threads, YouTube). Mỗi request được định tuyến sang đúng job và chạy
trong một tiến trình con (spider Scrapy) hoặc ngay trong tiến trình (YouTube, trang công khai, kênh Threads, số liệu bài).

## Các file

| File | Việc nó làm |
|---|---|
| `common.py` | Hằng số, lỗi job và đường dẫn dùng chung của consumer. |
| `dispatch.py` | Định tuyến một request crawl tới job tương ứng (kiểm tra huỷ / drain trước). |
| `loop.py` | Vòng lặp consumer Kafka theo từng nền tảng, làn ưu tiên Redis, khởi động lại khi lỗi. |
| `main.py` | Consumer của topic crawl_requests (Kafka): mỗi nền tảng một vòng lặp độc lập, mỗi request chạy một tiến trình con. |
| `process.py` | Chạy tiến trình con (spider / bootstrap), chuyển log, huỷ theo run_id hoặc theo nền tảng. |
| `sessions.py` | Đảm bảo phiên đăng nhập và các query đã bắt (search, comment, reply, page feed, reels) trước khi chạy job. |

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`jobs/`](jobs/README.md) | Một file mỗi loại request (search, comments, page_feed, ...) |

## Luồng

1. `main.py` khởi động `loop.run()`.
2. `loop.py` tạo vòng Kafka theo nền tảng + worker Redis; xử lý giờ yên lặng Meta, hạn mức giờ, nhường partition khi hết tài khoản.
3. Mỗi message -> `dispatch.py`: bỏ qua nếu job đã xong/bị huỷ/đang drain, rồi gọi job theo `request["type"]`.
4. `jobs/<job>.py` xin tài khoản/proxy (`runtime/`), đảm bảo phiên đăng nhập (`sessions.py`), chạy spider qua `process.py`.
5. Mã thoát của tiến trình con -> `common._raise_for_returncode` đổi thành lỗi có nghĩa (hết proxy, bị huỷ, gửi Kafka hỏng...).

## Muốn sửa / thêm thì

- Thêm loại request: job mới trong `jobs/`, thêm nhánh `elif request.get("type") == ...` trong `dispatch.py`.
- Đổi cách chia làn / hàng đợi: `loop.py` (mỗi hàng đợi Redis có `_recover_*` khôi phục job dở khi khởi động lại).
- Thêm mã thoát mới của spider: hằng số ở `constants/<nền tảng>.py`, xử lý ở `common._raise_for_returncode`.
