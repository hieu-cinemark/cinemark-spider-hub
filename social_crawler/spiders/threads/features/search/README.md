# social_crawler/spiders/threads/features/search

Spider tìm kiếm không mở trình duyệt (GraphQL qua curl_cffi); `extract.py` đổi node post của Threads thành bản ghi phẳng.

## Các file

| File | Việc nó làm |
|---|---|
| `extract.py` | Biến một response tìm kiếm GraphQL thô của Threads thành các bản ghi bài phẳng, dễ phân tích. |
| `search.py` | Spider tìm kiếm Threads không bao giờ mở trình duyệt: gọi thẳng endpoint GraphQL qua curl_cffi (giả dấu vân tay TLS của Chrome), dùng token mà `social_crawler.spiders.threads.auth.bootstrap` đã cache. |
