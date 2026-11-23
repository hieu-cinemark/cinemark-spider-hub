# scripts

Script một lần / chẩn đoán, không chạy tự động. Chạy từ gốc repo: `python -m scripts.<tên>` (trừ file `.sh`).

## Các file

| File | Việc nó làm |
|---|---|
| `account_timeline.py` | In dòng thời gian của một tài khoản: phiên, client, IP thoát, tín hiệu từ Facebook, nghỉ, tắt. |
| `audit_proxy_sources.py` | Khảo sát CHỈ ĐỌC các nguồn proxy trên D1. |
| `check_facebook_local.py` | Kiểm tra trước khi chạy Facebook trên Mac: Kafka và Redis qua tunnel đúng là của VPS. |
| `get_otp.py` | Helper chạy tay: in mã TOTP hiện tại của một dòng platform_accounts. |
| `measure_proxy_ip.py` | Đo IP thoát của một proxy nhiều lần để biết nó tĩnh hay tự xoay. |
| `open_account_browser.py` | Mở Chrome thật cho từng tài khoản qua đúng proxy đã gán để người vận hành đăng nhập/cập nhật phiên. |
| `preflight_browser.py` | Kiểm tra môi trường đăng nhập mà không chạm tài khoản hay Facebook: mở đúng trình duyệt mà mọi đường xác thực dùng (patchright, BROWSER_CHROMIUM_CHANNEL) trên about:blank, so phiên bản thật với hồ sơ dấu vân tay, và thử Redis. |
| `probe_fb_logged_out.py` | Test tải cho job post_metrics (cập nhật like/comment/share bài Facebook KHÔNG đăng nhập): gọi lại link bài đã biết bằng đúng code của job (spiders/facebook/features/post_metrics), qua một proxy trong platform_proxies, để tìm ... |
| `run_facebook_local.sh` | Chạy consumer Facebook trên Mac (xem docs/facebook-local.md). |
