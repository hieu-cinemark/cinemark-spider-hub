# social_crawler/spiders/facebook/features/search

Spider tìm kiếm không mở trình duyệt (GraphQL qua curl_cffi) và `extract.py` đổi phản hồi Relay lồng sâu thành bản ghi phẳng.

## Các file

| File | Việc nó làm |
|---|---|
| `extract.py` | Biến một response tìm kiếm GraphQL thô của Facebook (JSON Relay lồng sâu) thành các bản ghi phẳng, dễ phân tích: mỗi bài một dict và mỗi thực thể khác một dict (page, group, hashtag, video...). |
| `search.py` | Spider tìm kiếm Facebook không bao giờ mở trình duyệt: gọi thẳng endpoint GraphQL qua curl_cffi (giả dấu vân tay TLS của Chrome), dùng token mà `social_crawler.spiders.facebook.auth.bootstrap` đã cache. |
