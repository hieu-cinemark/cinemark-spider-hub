# social_crawler/spiders/youtube

Logic chép từ data-miner (13/11/2026), chạy trong tiến trình bởi `consumer/jobs/youtube.py`. Chỉ giữ phần đang dùng: tìm kiếm theo
nhiều bộ lọc, chi tiết video, bình luận + reply, video của kênh, transcript. Các nhánh chưa dùng (live, vị trí, playlist, shorts,
topic, thông tin kênh) đã xoá 17/11/2026 - lấy lại từ lịch sử git nếu cần.

## Các file

| File | Việc nó làm |
|---|---|
| `_compat.py` | Logger, lỗi `YouTubeStructureChangedError` và proxy mặc định (thay cho phần của data-miner). |
| `client.py` | Khoá API InnerTube, context, httpx client, resolve handle -> channel id. |
| `config.py` | Hằng số InnerTube và cấu hình HTTP. |
| `headers.py` | Header gửi YouTube. |
| `utils.py` | `parse_view_count`, `parse_relative_time`. |

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`channel/`](channel/README.md) | Video trong tab Video của một kênh |
| [`comment/`](comment/README.md) | Bình luận và reply của một video |
| [`detail/`](detail/README.md) | Chi tiết video (ngày đăng, like, mô tả) |
| [`search/`](search/README.md) | Tìm kiếm YouTube nhiều bộ lọc |
| [`shared/`](shared/README.md) | Hàm parse dùng chung giữa các module YouTube |
| [`transcript/`](transcript/README.md) | Phụ đề (transcript) của video |

## Lưu ý

Mọi module gọi API nội bộ InnerTube (hoặc đọc HTML) qua `httpx`. YouTube đổi cấu trúc phản hồi không báo trước nên code dùng
`.get()` / `or {}` xuyên suốt; khi đường parse hỏng thì ném `YouTubeStructureChangedError` (kèm đường dẫn khoá) - phải sửa
code, KHÔNG thử lại.

- **detail** (`get_video_meta`): ưu tiên trang watch (parse `ytInitialPlayerResponse` bằng đếm độ sâu ngoặc, header như trình duyệt, không cần API key);
  dự phòng InnerTube `/youtubei/v1/player` khi trang watch không 200, thiếu `ytInitialPlayerResponse` hoặc `LOGIN_REQUIRED`.
- **search** (`search_pages`): POST `/youtubei/v1/search` với tham số `sp` của từng bộ lọc, lật trang bằng `continuation`. Mã sắp xếp
  "upload_date" cũ không còn tác dụng từ 2026 - lấy video mới bằng bộ lọc ngày tải lên.
- **comment** (`get_video_comments`): POST `/youtubei/v1/next` lấy continuation, rồi phân trang; dữ liệu bình luận nằm ở
  `frameworkUpdates.entityBatchUpdate.mutations` (khoá theo `commentId`); reply lấy bằng `fetch_replies()` (sâu tối đa 2).
- **channel** (`get_channel_videos`): POST `/youtubei/v1/browse` vào tab Video (không có thì tab Home), lật trang bằng continuation.
- **transcript**: thư viện `youtube-transcript-api`; YouTube hay chặn IP datacenter nên job `consumer/jobs/youtube.py` thử proxy làn rồi IP máy.
