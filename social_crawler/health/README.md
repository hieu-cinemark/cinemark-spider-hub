# social_crawler/health

Bắt các lần sập *âm thầm* (mọi request trả HTTP 200 nhưng không có dữ liệu thật) bằng thăm dò lưu lượng thấp, và ping trực
tiếp các proxy đang bật để biết proxy nào chết trước khi job dùng tới.

## Các file

| File | Việc nó làm |
|---|---|
| `health_check.py` | Kiểm tra sức khoẻ chủ động, lưu lượng thấp cho các đường crawl gốc - nhằm bắt một lần sập toàn bộ *âm thầm* (mọi request đều trả HTTP 200 với header trông như thật nhưng dữ liệu rỗng/không có) trước khi có người nhận ra dashboard ... |
| `proxy_health_check.py` | Kiểm tra kết nối proxy chủ động - ping trực tiếp mọi dòng platform_proxies đang bật (một lần thăm dò kết nối đơn giản, không phải request đầy đủ ở cấp tính năng - xem health_check.py cho loại lỗi khác đó: "trông khoẻ nhưng không ... |
