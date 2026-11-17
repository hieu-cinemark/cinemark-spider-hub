# social_crawler/spiders/facebook

Facebook cần phiên đăng nhập thật: `auth/` bắt `doc_id` / `fb_dtsg` / `lsd` từ trình duyệt rồi phát lại request bằng HTTP
(curl_cffi) cho `features/`. Có đường không đăng nhập (`public_page`, `post_metrics`, bình luận công khai). Tài liệu vận hành
tài khoản: [../../../docs/facebook-accounts.md](../../../docs/facebook-accounts.md).

## Các file

| File | Việc nó làm |
|---|---|
| `items.py` | Dataclass bài / thực thể / bình luận Facebook (`FacebookPostItem`, `FacebookEntityItem`, `FacebookCommentItem`). |
| `response_utils.py` | Các helper nhỏ an toàn với dict/list để đào qua các response GraphQL lồng sâu của Facebook, dùng chung cho mọi tính năng dưới spiders/facebook. |
| `sdk_records.py` | Chuyển snapshot SDK đã lấy về sang hợp đồng Kafka/ingest thông thường. |

## Thư mục con

| Thư mục | Nội dung |
|---|---|
| [`auth/`](auth/README.md) | Đăng nhập, bắt phiên, client GraphQL, dự phòng SDK/V8 |
| [`features/`](features/README.md) | Các tính năng cào Facebook (mỗi thư mục một việc) |

## Lưu ý

Phần đăng nhập Facebook và Threads (`auth/bootstrap.py`, `auth/graphql_client.py`) có nhiều đoạn giống nhau; cố ý chưa gộp vì chỉ kiểm chứng được trên tài khoản thật (rủi ro checkpoint).
