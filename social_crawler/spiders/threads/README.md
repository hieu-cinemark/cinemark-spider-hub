# social_crawler/spiders/threads

Threads dùng cùng stack GraphQL với Facebook nhưng đăng nhập qua tài khoản Instagram. Có đường không đăng nhập (`public/`)
để đọc kênh và đường có đăng nhập (`auth/` + `features/`) cho tìm kiếm và reply.

## Các file

| File | Việc nó làm |
|---|---|
| `items.py` | Dataclass bài và bình luận Threads (`ThreadsPostItem`, `ThreadsCommentItem`). |

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`auth/`](auth/README.md) | Đăng nhập threads.com, bắt phiên, client GraphQL |
| [`features/`](features/README.md) | Tính năng cào Threads: tìm kiếm, reply, hồ sơ |
| [`public/`](public/README.md) | Đọc kênh Threads không đăng nhập |
