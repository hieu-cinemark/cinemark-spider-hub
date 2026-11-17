# social_crawler/core

Phần dùng chung không thuộc nền tảng nào. `env.py` là nơi DUY NHẤT gọi `load_dotenv()`; `logger.py` cài hợp đồng log chung
với cinemark-api (`app/core/logging.py` bên đó - sửa một bên thì sửa cả bên kia).

## Các file

| File | Việc nó làm |
|---|---|
| `crawl_outcome.py` | Kết quả có cấu trúc của một lượt `scrapy crawl` (02/11/2026): mã thoát 0 không có nghĩa lượt crawl thành công - exception trong async start() của spider chỉ được engine log ERROR, spider vẫn đóng với lý do "finished" và lệnh ... |
| `env.py` | Nơi duy nhất gọi load_dotenv(). Cả settings.py lẫn accounts.py đều cần .env được nạp trước khi đọc os.getenv(...) lúc import, nhưng không cái nào trông cậy được là cái kia đã chạy trước (cái nào cũng có thể được import riêng lẻ). ... |
| `fingerprint.py` | Giả lập trình duyệt/replay cũ, Threads và vài client phụ còn dùng. |
| `logger.py` | Cấu hình structlog cho spider-hub. Hợp đồng log dùng chung với cinemark-api (app/core/logging.py bên đó cài đặt cùng hợp đồng này - giữ hai bên đồng bộ): - Mọi dòng đều có: timestamp (ISO 8601, UTC), level, event, service, logger ... |
| `log_messages.py` | Danh mục câu tiếng Việt của mọi sự kiện log (`MESSAGES`). Thêm sự kiện = thêm một dòng; `tests/test_log_messages.py` bắt thiếu. |
