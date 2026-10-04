"""Truy cập Postgres (Supabase) cho cấu hình tài khoản/proxy - các giá trị này đổi đủ
thường xuyên (tài khoản bị tắt/thay, proxy bị xoay) nên việc sửa .env rồi restart mọi
tiến trình đọc nó (tiến trình con của bootstrap.py, tiến trình con của `scrapy crawl`,
service sống lâu crawl_request_consumer.py) không còn chấp nhận được. Thay đổi trên bảng
platform_accounts/platform_proxies có hiệu lực ngay ở lời gọi kế tiếp, không cần
restart.

Mỗi lời gọi một connection mới, không dùng pool: mọi chỗ gọi ở đây hoặc chạy trong một
tiến trình con sống ngắn (bootstrap.py chạy một lần mỗi lần refresh, tiến trình
`scrapy crawl` chạy một lần mỗi lượt crawl) hoặc gọi đủ hiếm (một lần lúc bắt đầu
crawl/session) nên quản lý vòng đời pool chỉ thêm phức tạp mà không có lợi ích thật.

Mỗi bảng (hoặc nhóm bảng) một module, không ORM/công cụ migrate:

  connection.py      connect() - nơi duy nhất xác định DATABASE_URL/LOCAL_DATABASE_URL
  accounts.py        platform_accounts(id, platform, account_id, password,
                     totp_secret, cookie, token, email, email_password,
                     enabled, created_at, updated_at, ...)
  proxies.py         platform_proxies(id, platform ['all' = dùng chung cho
                     mọi nền tảng], proxy_url, username, password,
                     login_use_proxy, enabled, created_at, updated_at, ...)
                     + ghim proxy qua platform_accounts.assigned_proxy_id
  relogin.py         phần platform_accounts mà luồng auto-login dùng (cookie
                     chết, needs_manual_login, last_relogin_*)
  config.py          filter_keywords, ai_providers, ai_settings - CRUD từ
                     dashboard (app/services/platform_config_db.py của
                     cinemark-api), ở đây chỉ đọc
  proxy_settings.py  proxy_settings + proxy_providers, có cache, dùng
                     DEFAULTS khi thiếu dòng/bảng
"""
