"""Truy cập D1 (cinemark-config-db) cho cấu hình tài khoản/proxy - các giá trị này đổi đủ thường xuyên (tài khoản bị tắt/thay, proxy
bị xoay) nên việc sửa .env rồi restart mọi tiến trình đọc nó không còn chấp nhận được. Thay đổi trên bảng
platform_accounts/platform_proxies có hiệu lực ngay ở lời gọi kế tiếp, không cần restart.

Mỗi bảng (hoặc nhóm bảng) một module, không ORM/công cụ migrate; schema do cinemark-api tạo:

  d1.py              client HTTP (query), connect() cho script, tiện ích thời gian/JSON
  d1_pool.py         toàn bộ SQL (SQLite) của accounts/proxies/account_events/proxy_providers
  accounts.py        platform_accounts: lấy/nhận/cập nhật/tắt dòng, kết quả đăng nhập và kiểm tra cookie
  proxies.py         platform_proxies (không ghim tài khoản: luân phiên theo nền tảng, runtime/proxy_lease.py)
  account_events.py  dòng thời gian theo tài khoản
  config.py          filter_keywords, ai_providers, ai_settings - ở đây chỉ đọc (cinemark-api ghi)
  proxy_settings.py  proxy_settings + proxy_providers, có cache, CHỈ ĐỌC - cinemark-api khai báo khoá/mặc định và ghi đủ dòng này
  script_connect.py  connect() cho script vận hành (alias của d1.connect)
"""
