# social_crawler/spiders/tiktok/features/search

Kế thừa spider hashtag, đổi endpoint sang `/api/search/general/full/`. Nghỉ khi IP bị phạt (`../../search_pause.py`).

## Các file

| File | Việc nó làm |
|---|---|
| `search.py` | Spider tìm kiếm từ khoá tự do của TikTok - cùng kiến trúc với tiktok_hashtag_search (curl_cffi, danh tính khách synthetic mới cho mỗi lần thử, ký ở local), chỉ khác endpoint: /api/search/general/full/ thay vì ... |
