# social_crawler/db

Mọi lời gọi tới Postgres của spider-hub. Mỗi lời gọi mở một connection mới (xem `connection.py` và `__init__.py` về lý do
không dùng pool). Spider-hub đọc phần lớn bảng cấu hình; cinemark-api giữ phía ghi các bảng do dashboard quản lý.

## Các file

| File | Việc nó làm |
|---|---|
| `account_events.py` | Dòng thời gian theo tài khoản (04/11/2026): tài khoản chết thì đọc lại được chuyện gì đã xảy ra, theo thứ tự. |
| `accounts.py` | platform_accounts: CRUD thường ngày của pool tài khoản - lấy/nhận/cập nhật/tắt dòng, ghi kết quả đăng nhập/phát lại và kiểm tra cookie (xem services/pool.py cho API acquire/release phía trên). |
| `config.py` | Các bảng cấu hình do dashboard quản lý mà phía này chỉ đọc: filter_keywords, ai_providers và ai_settings (cinemark-api giữ phía ghi). |
| `connection.py` | Connection Postgres (Supabase) cho mọi module trong package này - xem db/__init__.py để biết vì sao mỗi lời gọi một connection mới thay vì dùng pool. |
| `proxies.py` | platform_proxies: chọn proxy, sổ sách sức khoẻ/cooldown, và việc ghim cố định tài khoản<->proxy (platform_accounts.assigned_proxy_id) mà services/pool.acquire_proxy_for_account dựa vào. |
| `proxy_settings.py` | Hành vi proxy tinh chỉnh được, sửa từ trang Settings của dashboard (/settings/proxy và /settings/proxy/providers của cinemark-api) thay vì gán cứng rải rác trong pool.py, db.py, proxy_provider.py, proxy_health_check.py, ... |

## Muốn sửa / thêm thì

- Thêm cài đặt chỉnh từ dashboard: thêm khoá + mặc định trong `proxy_settings.py`, thêm vào `ProxySettings` (cinemark-api
  `app/schemas/settings.py`) và form dashboard; đọc bằng `get_setting("khoá")` (cache 60 giây, đổi trên dashboard có hiệu lực không cần restart).
- Thêm cột/bảng: sửa schema ở cinemark-api (nơi tạo bảng) rồi `scripts/dev_db_schema.sql` cho DB local.
