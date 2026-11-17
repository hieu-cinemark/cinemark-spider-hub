# social_crawler/runtime

Tầng điều phối tài nguyên dùng chung: job xin tài khoản và proxy ở đây, ghi lại kết quả để circuit-breaker biết, và bị giới hạn
bởi hạn mức/nghỉ để không bắt chước hành vi máy (nguyên nhân chính khiến Meta khoá tài khoản).

## Các file

| File | Việc nó làm |
|---|---|
| `account_lease.py` | Quyền dùng độc quyền một tài khoản Meta xuyên suốt đăng nhập / Restore / crawl / nuôi / kiểm tra cookie (02/11/2026). |
| `deep_cursor.py` | Đào bài cũ lùi dần theo từng từ khoá Facebook (27/10/2026) - phần "deep" của hạn mức (services/search_budget.py). |
| `facebook_pacing.py` | Khoảng nghỉ cố định theo từng tài khoản; mỗi request thật đều ghi lại lúc xong. |
| `facebook_run_budget.py` | Giới hạn thời gian làm việc liên tục có đăng nhập qua nhiều job mà không bỏ con trỏ phân trang. |
| `lanes.py` | Làn Facebook chạy song song (24/10/2026): FACEBOOK_LANES consumer Kafka cùng group (topic crawl_requests cần >= số làn partition - message có key facebook:<keyword_id> nên tự rải đều), mỗi làn giữ CỐ ĐỊNH một tài khoản tới ... |
| `pool.py` | Pool tài khoản và proxy: chọn có xét circuit-breaker và ghi kết quả, nằm trên các bảng platform_accounts/platform_proxies (xem db/accounts.py, db/proxies.py). |
| `proxy_lease.py` | Chọn proxy cho job có tài khoản (13/11/2026): mọi proxy là proxy xoay, chia theo danh sách của từng nền tảng, luân phiên. |
| `search_budget.py` | Hạn mức request Facebook mỗi tài khoản mỗi ngày (24/10/2026, chống checkpoint): một tài khoản gánh hàng nghìn request / ngày là hành vi không người thật nào có - tài khoản nào cũng dính checkpoint, chỉ sớm hay muộn. |
| `task_queue.py` | Helper hàng đợi thu thập ở phía consumer - cùng các key Redis mà cinemark-api/app/services/task_queue.py ghi khi một message crawl_requests được publish. |

## Luồng

Một job có tài khoản: `account_lease` (giữ độc quyền tài khoản) -> `proxy_lease` (chọn proxy xoay theo danh sách nền tảng) ->
`search_budget` / `facebook_pacing` / `facebook_run_budget` (hạn mức ngày, khoảng nghỉ, giờ làm việc liên tục) -> chạy spider ->
`pool` ghi kết quả (cooldown tài khoản/proxy khi lỗi).

## Muốn sửa / thêm thì

- Đổi hạn mức/nghỉ: `search_budget.py`, `facebook_pacing.py`, `facebook_run_budget.py` (một phần lấy từ `proxy_settings` trên dashboard).
