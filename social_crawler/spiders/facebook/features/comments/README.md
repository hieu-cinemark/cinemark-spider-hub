# social_crawler/spiders/facebook/features/comments

Spider bình luận Facebook không mở trình duyệt: gọi GraphQL bằng query đã bắt; `public_comments.py` là đường ẩn danh có phân trang.

## Các file

| File | Việc nó làm |
|---|---|
| `comments.py` | Spider comment Facebook không bao giờ mở trình duyệt: lấy comment của một bài qua curl_cffi, dùng query mà `bootstrap_comments()` (social_crawler.spiders.facebook.auth.bootstrap) đã cache. |
| `extract.py` | Biến một response GraphQL danh sách comment thô của Facebook thành các bản ghi phẳng, mỗi comment một bản ghi. |
| `public_comments.py` | Bình luận Facebook chỉ công khai và phản hồi tầng 1 có phân trang, giữ hợp đồng Kafka hiện có. |
