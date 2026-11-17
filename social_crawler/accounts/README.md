# social_crawler/accounts

Việc chăm sóc tài khoản mạng xã hội sau khi đã có trong bảng `platform_accounts`: lướt feed nhẹ cho "nuôi", điều kiện
warm-up, kiểm tra cookie trước khi dùng, và dò cảnh báo của Meta (nấc trước checkpoint). Không chọn tài khoản cho job -
việc đó của `runtime/pool.py` và `runtime/account_lease.py`.

## Các file

| File | Việc nó làm |
|---|---|
| `account_warning.py` | Dò cảnh báo của Meta trên trang đang mở ("Chúng tôi nghi ngờ hành vi tự động", "Bạn tạm thời bị chặn"...) - 30/10/2026. |
| `nurture.py` | Các phiên lướt feed nhẹ cho tài khoản trong pool của Facebook, Threads, và (khi bật) TikTok. |
| `session_quality.py` | Kiểm tra cookie Facebook trước request đầu tiên (04/11/2026). |
| `warmup.py` | Ngày nuôi và điều kiện warm-up cho Threads. |

## Luồng

Job `nurture` (`consumer/jobs/accounts.py`) gọi `nurture.py`; `session_quality.py` chạy trước request đầu của phiên Facebook;
`account_warning.py` được gọi khi trang đang mở có thể hiện cảnh báo của Meta.

## Muốn sửa / thêm thì

- Đổi lịch/điều kiện nuôi: `warmup.py` (Threads) và biến cấu hình của `nurture.py`. Luật nghỉ/tắt tài khoản nằm ở `db/accounts.py`.
