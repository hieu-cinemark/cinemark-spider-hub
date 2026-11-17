# social_crawler/spiders/common

Lớp cơ sở GraphQL Comet/Barcelona dùng cho cả Facebook lẫn Threads, lọc bài lạc đề, phát hiện lỗi dồn dập, log bài đã cào.

## Các file

| File | Việc nó làm |
|---|---|
| `comet_graphql_client.py` | Lớp cơ sở dùng chung cho client GraphQL của Facebook và Threads - cả hai chạy trên cùng stack GraphQL Comet/Barcelona (đã xác nhận với một request BarcelonaPostPageStrongIdTargetQuery thật bắt được trên threads.com), nên gần như ... |
| `error_alerts.py` | Phát hiện đợt lỗi dồn dập theo cửa sổ trượt cho các lỗi spider tạm thời (RateLimitedError/NetworkError trên Facebook/Threads/TikTok) - giống cách ingest_consumer._note_drop của cinemark-api: một lần xảy ra đơn lẻ vốn đã được ... |
| `keyword_filter.py` | Lọc bài lạc đề ngay trong spider tìm kiếm Facebook/Threads (2026-10-15). |
| `post_log.py` | Một dòng log cho mỗi bài crawl được và đã gửi lên Kafka, để terminal / consumer.log / trang Nhật ký của dashboard thấy ngay nội dung đang được thu thập (trước đây chỉ có page_crawled với số lượng, phải mở D1 mới biết bài nào lọt ... |
| `search_query.py` | Helper text query tìm kiếm dùng chung cho các spider tìm kiếm facebook/threads - để ngoài từng spider để cả hai đồng bộ ở điểm này thay vì mỗi bên tự viết lại. |
| `comet_errors.py` | Lỗi, mã lỗi Facebook/Threads và nhận diện checkpoint của client GraphQL Comet. |
| `comet_parse.py` | Hàm thuần của client GraphQL Comet: parse phản hồi (kể cả luồng), gộp dữ liệu trả dần, dựng biến theo mẫu, chuẩn hoá vân tay phiên. |
| `comet_throttle.py` | Bóp nhịp request của client GraphQL Comet (mixin). |
| `comet_comments.py` | Lấy bình luận và reply của client GraphQL Comet (mixin). |

## Muốn sửa / thêm thì

- Sửa hành vi chung của client GraphQL ở `comet_graphql_client.py` (ảnh hưởng cả Facebook lẫn Threads - chạy test cả hai).
