# social_crawler/spiders/tiktok/auth

Chọn dòng `platform_accounts` cho client, làm mới `device_id`/`odin_id`, dựng cookie. Lưu ý: ký ở local chỉ hợp lệ cho request khách, nên cookie đăng nhập hiện không dùng để cào.

## Các file

| File | Việc nó làm |
|---|---|
| `accounts.py` | Chọn dòng platform_accounts (platform='tiktok') mà một lần chạy client đóng vai - query mới từ Supabase ở mỗi lời gọi (xem social_crawler/db/accounts.py), cùng kiểu với auth/accounts.py của facebook/threads. |
| `bootstrap.py` | Làm mới device_id/odin_id của một dòng platform_accounts tiktok - danh tính tin cậy thiết bị mà TikTokClient dùng để ký mọi request (xem docstring module của constants/tiktok.py và client.py). |
| `cookies.py` | Dựng storage_state Playwright từ trường `cookie` thô của một dòng platform_accounts tiktok - cùng ý tưởng với facebook.auth.cookies.build_storage_state_from_cookies / threads.auth.cookies.build_storage_state_from_cookies, nhưng ... |
| `identity.py` | Helper chọn danh tính TikTok web mà không cần Playwright. |
