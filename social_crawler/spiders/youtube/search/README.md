# social_crawler/spiders/youtube/search

`search_pages` lật trang kết quả; mỗi từ khoá chạy qua nhiều bộ lọc (`SEARCH_FILTERS`) vì mỗi bộ lọc cho tập kết quả khác nhau.

## Các file

| File | Việc nó làm |
|---|---|
| `search.py` | `search_pages`: lật trang kết quả, trích video thường và Shorts. |
| `search_constants.py` | Bảng bộ lọc `SEARCH_FILTERS` (tham số `sp`). |
