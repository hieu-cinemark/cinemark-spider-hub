# Facebook có tài khoản chạy trên Mac

Từ 03/11/2026 phiên Facebook được tạo bằng Chrome trên Mac, nên consumer dùng tài khoản cũng chạy trên Mac: phiên có hệ điều hành khác máy chạy bị từ chối.

Từ 10/11/2026 đường cào HTTP (curl_cffi) không còn bắt buộc cùng hệ điều hành: curl_cffi giả vân tay Chrome macOS (`chromeNNN_macos`) cả khi chạy trên Linux, nên phiên bắt trên Mac chạy được từ VPS với cùng UA và proxy ghim. Mở trình duyệt thật (đăng nhập, nuôi) vẫn phải đúng hệ điều hành của phiên. Muốn VPS cào Facebook có tài khoản: thêm `facebook` vào `CRAWLER_PLATFORMS` của VPS và tắt consumer Facebook trên Mac (mỗi phiên chỉ một máy dùng). Threads, TikTok, comment ẩn danh và đọc page vẫn chạy trên VPS. Hai bên dùng chung Kafka, Redis và DB của production, nên ingest, dashboard và nút Dừng không đổi.

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
