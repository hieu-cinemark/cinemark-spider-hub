# social_crawler/spiders/threads/features/profiles

Worker hồ sơ kênh cho trang "Xuất thông tin theo kênh" của cinemark; cinemark-api chọn kênh cần hỏi qua job `profiles`.

## Các file

| File | Việc nó làm |
|---|---|
| `extract.py` | Parse response query hovercard hồ sơ Threads (BarcelonaUsernameHovercardImplDirectQuery) - xem ThreadsGraphQLClient.fetch_profile. |
| `profiles.py` | Worker hồ sơ kênh Threads (follower) cho trang "Xuất thông tin theo kênh" của cinemark - từ 20/10/2026. |
