"""Truy cập Postgres (Supabase) cho cấu hình tài khoản/proxy - các giá trị này đổi đủ
thường xuyên (tài khoản bị tắt/thay, proxy bị xoay) nên việc sửa .env rồi restart mọi
tiến trình đọc nó (tiến trình con của bootstrap.py, tiến trình con của `scrapy crawl`,
service sống lâu crawl_request_consumer.py) không còn chấp nhận được. Thay đổi trên bảng
platform_accounts/platform_proxies có hiệu lực ngay ở lời gọi kế tiếp, không cần
restart.

Connection được tái dùng trong tiến trình (connection.py: kho nhỏ, nhàn rỗi tối đa 20 giây) - mở connection mới cho từng lời
gọi từng là nguồn egress Supabase lớn khi consumer chạy liên tục.

Mỗi bảng (hoặc nhóm bảng) một module, không ORM/công cụ migrate:

  connection.py      connect() - nơi duy nhất xác định DATABASE_URL/LOCAL_DATABASE_URL
  accounts.py        platform_accounts(id, platform, account_id, password,
                     totp_secret, cookie, token, email, email_password,
                     enabled, created_at, updated_at, ...)
  proxies.py         platform_proxies(id, platform [proxy gán cho nền tảng
                     nào thì chỉ nền tảng đó dùng], proxy_url, username, password,
                     login_use_proxy, enabled, created_at, updated_at, ...)
                     (không ghim tài khoản: luân phiên theo nền tảng, runtime/proxy_lease.py)
  config.py          filter_keywords, ai_providers, ai_settings - CRUD từ
                     dashboard (app/services/platform_config_db.py của
                     cinemark-api), ở đây chỉ đọc
  proxy_settings.py  proxy_settings + proxy_providers, có cache, CHỈ ĐỌC -
                     cinemark-api khai báo khoá/mặc định và ghi đủ dòng này
"""
