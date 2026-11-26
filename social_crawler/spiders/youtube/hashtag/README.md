# social_crawler/spiders/youtube/hashtag

Video theo hashtag (`youtube.com/hashtag/<tên>`), không đăng nhập. Job YouTube dùng khi từ khoá bắt đầu bằng `#`
(`consumer/jobs/youtube.py`, `search_sources`).

## Các file

| File | Việc nó làm |
|---|---|
| `hashtag.py` | `hashtag_params` (tham số protobuf), `hashtag_pages` (lật trang một tab: `all` = video thường, `shorts` = Shorts). |

## Lưu ý

- Hai tab không trùng nhau, phải đọc cả hai.
- Shorts chỉ có tiêu đề và lượt xem làm tròn; kênh, ngày đăng, like lấy ở API chi tiết như mọi video mới.
- YouTube không trả hết số video hashtag nó báo (đo 26/11/2026: #traigiamhanhphuc báo 527, lấy được khoảng 228).
