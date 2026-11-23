# social_crawler/db

Mọi lời gọi tới D1 (`cinemark-config-db`) của spider-hub, qua HTTP API của Cloudflare (`d1.py`, SQL ở `d1_pool.py`). Spider-hub đọc phần lớn bảng cấu hình; cinemark-api giữ phía ghi các bảng do dashboard quản lý.

## Các file

| File | Việc nó làm |
|---|---|
| `account_events.py` | Dòng thời gian theo tài khoản (04/11/2026): tài khoản chết thì đọc lại được chuyện gì đã xảy ra, theo thứ tự. |
| `accounts.py` | platform_accounts: CRUD thường ngày của pool tài khoản - lấy/nhận/cập nhật/tắt dòng, ghi kết quả đăng nhập/phát lại và kiểm tra cookie (xem services/pool.py cho API acquire/release phía trên). |
| `config.py` | Các bảng cấu hình do dashboard quản lý mà phía này chỉ đọc: filter_keywords, ai_providers và ai_settings (cinemark-api giữ phía ghi). |
| `d1.py`, `d1_pool.py`, `script_connect.py` | Client HTTP tới D1, toàn bộ SQL của accounts/proxies/events/providers, và connect() cho script vận hành. |
| `proxies.py` | platform_proxies: chọn proxy và sổ sách sức khoẻ/cooldown (không ghim tài khoản; luân phiên ở runtime/proxy_lease.py). |
| `proxy_settings.py` | Hành vi proxy tinh chỉnh được, sửa từ trang Settings của dashboard (/settings/proxy và /settings/proxy/providers của cinemark-api) thay vì gán cứng rải rác trong pool.py, db.py, proxy_provider.py, proxy_health_check.py, ... |

## Muốn sửa / thêm thì

- Thêm cài đặt chỉnh từ dashboard: CHỈ thêm ở cinemark-api (`ProxySettings` trong `app/schemas/settings.py`, khoá + mặc định +
  giới hạn) và form dashboard; deploy cinemark-api trước (nó ghi khoá mới vào dòng `proxy_settings` khi khởi động), rồi đọc ở đây bằng
  `get_setting("khoá")` (cache 60 giây). Cập nhật bản chụp `tests/fixtures/proxy_settings.json` cho test.
- Thêm cột/bảng: sửa schema ở cinemark-api (`config_db/d1_accounts.py`, nơi tạo bảng) và DDL mẫu trong `tests/test_d1_pool.py`.
