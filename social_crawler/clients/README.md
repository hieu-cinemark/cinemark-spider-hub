# social_crawler/clients

Lớp mỏng bọc các dịch vụ bên ngoài để phần còn lại không phụ thuộc thư viện cụ thể. Spider và job chỉ `import` từ đây,
không tự mở kết nối.

## Các file

| File | Việc nó làm |
|---|---|
| `ai.py` | Client AI của spider-hub - mọi lời gọi đi qua nhà cung cấp AI đang chọn (tương thích OpenAI), anh em với app/ai/ của cinemark-api. |
| `kafka.py` | `KafkaPublisher`: gửi bài/bình luận lên topic `raw_posts` / `raw_comments` (spider gọi `await kafka.publish(topic, key, value)`). |
| `proxy_provider.py` | Nguồn proxy: mỗi nguồn là MỘT link reset + MỘT key. |
| `redis.py` | `RedisCache`: get/set/sadd/incr... với key tự gắn tiền tố `social_crawler:`, thử lại khi Redis lỗi tạm; `enable_dedupe_cache` bật tập chống trùng của spider. |
| `telegram.py` | `send_telegram_message`: gửi cảnh báo Telegram, bỏ qua khi chưa cấu hình token. Test không bao giờ gửi thật (guard trong `tests/conftest.py`). |

## Muốn sửa / thêm thì

- Thêm dịch vụ ngoài mới: một file ở đây, không gọi thẳng từ spider.
