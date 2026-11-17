# social_crawler/consumer/jobs

Mỗi file là một loại việc `dispatch.py` có thể giao. Job chịu trách nhiệm xin tài khoản/proxy, kiểm tra điều kiện, chạy
spider và đổi kết quả thành thành công/bỏ qua/thử lại/thất bại.

## Các file

| File | Việc nó làm |
|---|---|
| `accounts.py` | Job tài khoản: làm mới token / danh tính, import cookie, nuôi tài khoản, kiểm tra cookie. |
| `channel_videos.py` | Job danh sách video một kênh TikTok (type="channel_videos"). |
| `comments.py` | Job cào bình luận của một bài (type="comments"). |
| `page_feed.py` | Job cào bảng tin + tab Reels của một Page Facebook (type="page_feed"). |
| `post_metrics.py` | Job cập nhật like/comment/share bài Facebook theo link, KHÔNG đăng nhập (type="post_metrics", 25/10/2026). |
| `profiles.py` | Job hồ sơ / follower kênh Threads (type="profiles"). |
| `public_page.py` | Job đọc một page Facebook không đăng nhập (type="public_page", 03/11/2026), chạy song song ở các worker riêng trong consumer/loop.py (hàng đợi crawl_public_pages:facebook). |
| `search.py` | Job tìm kiếm theo từ khoá / hashtag (type mặc định). |
| `threads_channel.py` | Job đọc kênh Threads không đăng nhập (type="threads_channel", 13/11/2026), chạy ở worker riêng trong consumer/loop.py (hàng đợi crawl_public_pages:threads), cùng kiểu đọc page Facebook không đăng nhập (jobs/public_page.py). |
| `youtube.py` | Job YouTube (13/11/2026): tìm kiếm theo từ khoá, bình luận một video, video của một kênh - không tài khoản, chạy trong tiến trình (InnerTube qua httpx, logic chép từ data-miner: spiders/youtube/). |

## Luồng

| `request["type"]` | Job | Chạy bằng |
|---|---|---|
| (không có) / `search` | `search.py` | tiến trình con Scrapy (TikTok gộp từ khoá + hashtag) |
| `comments` | `comments.py` | tiến trình con Scrapy |
| `channel_videos` | `channel_videos.py` | tiến trình con Scrapy (TikTok) |
| `page_feed` | `page_feed.py` | tiến trình con Scrapy (Facebook, có đăng nhập) |
| `post_metrics` | `post_metrics.py` | trong tiến trình, không đăng nhập |
| `public_page` | `public_page.py` | trong tiến trình, worker riêng, không đăng nhập |
| `threads_channel` | `threads_channel.py` | trong tiến trình, worker riêng, không đăng nhập |
| `profiles` | `profiles.py` | worker hồ sơ kênh Threads (lấy handle từ hàng đợi Redis do cinemark-api nạp) |
| `nurture`, `refresh_token`, `cookie_import` | `accounts.py` | tiến trình con / trong tiến trình |
| YouTube (mọi loại) | `youtube.py` | trong tiến trình, theo làn |

## Lưu ý

`threads_channel.py` và `public_page.py` có vòng gửi bài trông giống nhau nhưng khác ngữ nghĩa (Threads báo thử lại ngay khi bài
đầu không gửi được; Facebook gom lại rồi mới quyết định và có chế độ `resend`) nên cố ý để riêng - đừng gộp khi chưa so lại hành vi.
