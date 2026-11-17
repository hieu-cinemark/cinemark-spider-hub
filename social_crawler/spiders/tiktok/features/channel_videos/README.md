# social_crawler/spiders/tiktok/features/channel_videos

Gọi `/api/post/item_list/`; `extract.py` đổi phản hồi thành bản ghi video.

## Các file

| File | Việc nó làm |
|---|---|
| `extract.py` | Biến một response /api/post/item_list/ thô của TikTok thành các bản ghi video phẳng, dễ phân tích. |
| `search.py` | Spider danh sách video kênh/người dùng TikTok - gọi thẳng /api/post/item_list/ qua curl_cffi với một danh tính khách synthetic mới, ký ở local, giống tiktok_hashtag_search. |
