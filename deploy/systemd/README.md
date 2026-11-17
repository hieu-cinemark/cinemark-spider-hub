# deploy/systemd

`spider-hub-crawl-consumer.service` chạy `python -m social_crawler.consumer.main`; `install.sh` cài vào `/etc/systemd/system`.

## Các file

| File | Việc nó làm |
|---|---|
| `install.sh` | Cài service và bật khi khởi động. |
| `spider-hub-crawl-consumer.service` | Unit chạy consumer. |
