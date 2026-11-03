# Facebook có tài khoản chạy trên Mac

Từ 03/11/2026 phiên Facebook được tạo bằng Chrome trên Mac, nên consumer dùng tài khoản cũng chạy trên Mac: phiên có hệ điều hành khác máy chạy bị từ chối. Threads, TikTok, comment ẩn danh và đọc page vẫn chạy trên VPS. Hai bên dùng chung Kafka, Redis và DB của production, nên ingest, dashboard và nút Dừng không đổi.

`CRAWLER_PLATFORMS` chọn vòng lặp chạy trên mỗi máy. Rỗng là tất cả.

| Máy | `CRAWLER_PLATFORMS` |
| --- | --- |
| VPS (`/opt/spider-hub/.env`) | `facebook_public,threads,tiktok` |
| Mac (do script đặt) | `facebook` |

## Chạy

```sh
scripts/run_facebook_local.sh
```

Script dừng Kafka local để nhường cổng 9092, mở tunnel SSH tới Kafka và Redis của VPS, kiểm tra đúng là VPS rồi mới chạy. Kafka VPS quảng bá địa chỉ `localhost:9092` nên không dùng được cổng khác; để Kafka local chạy thì tin nhắn sẽ đi nhầm vào đó. Thoát bằng Ctrl+C. Muốn dùng lại Kafka local: `docker start cinemark-workspace-kafka-1`.

Trong lúc script chạy, các lệnh spider-hub khác trên Mac mà không đặt `REDIS_PORT=16379` vẫn dùng Redis local.

## Khi Mac tắt hoặc mất mạng

Job Facebook nằm chờ trong Kafka. Job tìm kiếm và `page_feed` quá hạn bị bỏ theo `FACEBOOK_STALE_JOB_HOURS`; lượt lịch sau xếp lại. Comment ẩn danh và đọc page không bị ảnh hưởng. Trong thời gian đó, job search Facebook còn chờ dưới hạn trên sẽ làm comment ẩn danh và đọc page nhường (`FB_PUBLIC_YIELD_TO_SEARCH`), nên có thể phải đặt biến này về 0 nếu Mac tắt lâu.

Chưa chạy thử với tài khoản thật. Chạy từ Mac giữ phiên và proxy nhất quán, nhưng chưa có bằng chứng giảm checkpoint.
