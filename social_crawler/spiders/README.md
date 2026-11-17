# social_crawler/spiders

Mỗi nền tảng một package: `auth/` (đăng nhập, bắt phiên, client GraphQL), `features/<tính năng>/` (spider + `extract.py`),
`items.py` (dạng dữ liệu). `common/` chứa phần dùng chung giữa nền tảng. Spider Scrapy được nạp theo `name` (không ai `import`
trực tiếp lớp spider - đừng xoá chỉ vì công cụ báo "không dùng").

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`common/`](common/README.md) | Phần dùng chung giữa Facebook, Threads, TikTok |
| [`facebook/`](facebook/README.md) | Cào Facebook: đăng nhập, GraphQL, tìm kiếm, bình luận, page, số liệu bài |
| [`threads/`](threads/README.md) | Cào Threads: đăng nhập, tìm kiếm, reply, hồ sơ kênh, đọc kênh công khai |
| [`tiktok/`](tiktok/README.md) | Cào TikTok: client có ký, hashtag, từ khoá, bình luận, video kênh |
| [`youtube/`](youtube/README.md) | Cào YouTube qua InnerTube (httpx), không tài khoản |

## Luồng

Quy ước chung: `search.py` / `comments.py` / ... lấy phản hồi thô -> `extract.py` biến thành bản ghi phẳng -> spider gửi
`KafkaPublisher.publish(raw_posts | raw_comments)`; bản ghi đã gửi ghi vào tập chống trùng Redis để lần sau bỏ qua.
