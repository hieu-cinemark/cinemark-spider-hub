# social_crawler/spiders/facebook/features/page_reels

Chỉ có `extract.py`: biến phản hồi tab Reels của một Page thành bản ghi bài (dùng bởi `page_feed`).

## Các file

| File | Việc nó làm |
|---|---|
| `extract.py` | Trích reel từ response tab Reels của một Page (ProfileCometAppCollectionReelsRendererPaginationQuery, 2026-10-21): data.node.aggregated_fb_shorts.edges[].profile_reel_node.node là một Story - post_id (khớp external_id bài ... |
