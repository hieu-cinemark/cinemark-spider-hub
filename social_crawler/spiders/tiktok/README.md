# social_crawler/spiders/tiktok

TikTok không dùng trình duyệt cho đường cào: `client.py` ký mọi request ở local (X-Gnarly + X-Dynosaur trong `signature/`) với danh tính
khách synthetic mới cho mỗi lần thử. Tìm kiếm từ khoá bị TikTok phạt theo IP nên có cơ chế nghỉ (`search_pause.py`).

## Các file

| File | Việc nó làm |
|---|---|
| `client.py` | Client HTTP thường (không trình duyệt) cho các endpoint có ký của TikTok. |
| `items.py` | Dataclass video (`TikTokVideoItem`, `TikTokChannelVideoItem`) và bình luận TikTok. |
| `search_pause.py` | Nghỉ tìm từ khoá TikTok khi IP bị phạt (16/11/2026). |

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`auth/`](auth/README.md) | Danh tính TikTok: chọn tài khoản, device_id/odin_id, cookie |
| [`features/`](features/README.md) | Tính năng cào TikTok |
| [`signature/`](signature/README.md) | Bộ ký request TikTok (X-Gnarly, X-Dynosaur) |
