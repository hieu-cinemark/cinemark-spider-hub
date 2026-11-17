# social_crawler/spiders/tiktok/features/hashtag_search

Spider chính của TikTok: `/api/challenge/item_list/`. Lớp cơ sở của `features/search`.

## Các file

| File | Việc nó làm |
|---|---|
| `extract.py` | Biến một response /api/challenge/item_list/ thô của TikTok thành các bản ghi video phẳng, dễ phân tích. |
| `search.py` | Spider tìm kiếm hashtag TikTok không bao giờ mở trình duyệt: gọi thẳng /api/challenge/item_list/ qua curl_cffi (giả dấu vân tay TLS của Chrome), ký mọi request ở local bằng một X-Gnarly mới tính (xem signature/gnarly.py). |
