# social_crawler/spiders/facebook/features

Mỗi thư mục con là một tính năng: spider + `extract.py` biến phản hồi GraphQL/HTML thành bản ghi phẳng.

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`comments/`](comments/README.md) | Bình luận của một bài (có đăng nhập và công khai) |
| [`post_metrics/`](post_metrics/README.md) | Số like/comment/share của bài đã biết link, không đăng nhập |
| [`public_page/`](public_page/README.md) | Đọc feed Page công khai bằng phiên khách |
| [`search/`](search/README.md) | Tìm kiếm Facebook theo từ khoá |
