# social_crawler/spiders/tiktok/features

Mỗi thư mục con: spider + `extract.py` cho một endpoint TikTok.

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`channel_videos/`](channel_videos/README.md) | Video đã đăng của một kênh |
| [`comments/`](comments/README.md) | Bình luận và reply của một video |
| [`hashtag_search/`](hashtag_search/README.md) | Tìm video theo hashtag (và BFS hashtag liên quan) |
| [`search/`](search/README.md) | Tìm video theo từ khoá tự do |
