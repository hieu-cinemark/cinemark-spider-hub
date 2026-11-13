# services

YouTube data-fetching layer. All services call the InnerTube internal API (or scrape HTML) via `httpx`. Safe navigation (`.get()`, `or {}`) is used throughout because YouTube changes its response structure without notice.

## Error Types

| Exception | Meaning | Retry? |
|-----------|---------|--------|
| `YouTubeStructureChangedError` | Response structure changed — parse path broken | No — fix code first |
| `CrawlNetworkError` | Network/HTTP failure | Yes |

## Service Files

### `detail.py` — Video detail

Entry point: `get_video_detail(video_id, proxy)`

Two-strategy waterfall:

1. **Watch page** (`GET youtube.com/watch?v=<id>`): parses `ytInitialPlayerResponse` from HTML using brace-depth counting. Most reliable — uses real browser headers, no API key needed.
2. **InnerTube player API** (`POST /youtubei/v1/player`): fallback when watch page returns non-200, `ytInitialPlayerResponse` is absent, or playability status is `LOGIN_REQUIRED`.

Returns: `video_id`, `title`, `author`, `length_seconds` (string), `views` (string), `is_live_content`, `formats`, `adaptive_formats`.

---

### `search.py` — YouTube search

Entry point: `search_youtube(query, max_results, proxy, sort)`

- Posts to InnerTube `/youtubei/v1/search` with optional `params` for sort order.
- Sort options: `relevance`, `upload_date`, `view_count`, `rating` (mapped to InnerTube param bytes).
- Paginates via `continuation` token from `continuationItemRenderer` until `max_results` reached.
- Raises `YouTubeStructureChangedError` if `sectionListRenderer.contents` is missing from the first response.

---

### `trending.py` — Trending videos

Entry point: `get_trending_videos(proxy, max_results, gl, hl)`

Two strategies (automatic fallback):

1. **HTML scrape** (`GET /feed/trending`): parses `ytInitialData`, walks `richGridRenderer` / `sectionListRenderer`, paginates via continuation. Requires residential proxy — datacenter IPs receive the home feed (`feedNudgeRenderer`) instead of the trending page.
2. **Search-by-view-count fallback**: posts to InnerTube search with `params=SORT_VIEW_COUNT` and empty query. Works from any IP. Used when HTML scrape returns an empty list or raises `YouTubeStructureChangedError`.

---

### `live.py` — Live videos

Entry point: `get_all_live_videos(q, proxy, max_results)`

Posts to InnerTube search with `params=SEARCH_FILTER_LIVE`. Paginates via continuation token. Raises `YouTubeStructureChangedError` if `sectionListRenderer.contents` is absent.

---

### `comment.py` — Comments and replies

Entry point: `get_video_comments(video_id, proxy, max_comments)`

- First POST to `/youtubei/v1/next` with `videoId` to get the comment continuation token (tries two response paths: `onResponseReceivedEndpoints` and `twoColumnWatchNextResults`).
- Subsequent POSTs with `continuation` token fetch comment pages.
- Comment data is parsed from `frameworkUpdates.entityBatchUpdate.mutations` (entity map keyed by `commentId`).
- For each `commentThreadRenderer` that has a reply continuation token, replies are fetched via `fetch_replies()` (max depth 2).

---

### `shorts.py` — Shorts feed

Entry point: `get_shorts_feed(proxy, max_results)`

Two strategies (run in sequence, deduped by `video_id`):

1. **Search shelf**: searches `#shorts`, `shorts funny`, `shorts viral`, `shorts trending 2024`. Extracts `reelItemRenderer` and `shortsLockupViewModel` items from results via recursive `_find_reel_item_renderers`.
2. **Reel session** (`reel/reel_item_watch`): used if search yields fewer results than `max_results`. Walks the trending reel feed using `pos_params` from `replacementEndpoint.reelWatchEndpoint`. Stops on 5 consecutive duplicates, 3 stale `pos_params`, or non-`REEL_ITEM_WATCH_STATUS_SUCCEEDED` status.

Data stored in separate `shorts` table — not `videos`. Shorts lack `channel_id`, `publishedTime`, and `descriptionSnippet`.

---

### `location.py` — Region-targeted search

Entry point: `get_videos_by_region(gl, hl, query, proxy, max_results)`

Overrides the InnerTube context `client.gl` and `client.hl` (plus `timeZone` mapped from `gl`) to get region-relevant results. Does **not** use `location`/`locationRadius` params — YouTube InnerTube ignores them. Paginates via continuation. Deduplicates by `video_id` before returning.

---

### `channel.py` — Channel videos

Entry point: `get_channel_videos(channel_id, proxy, max_results)`

- POSTs to InnerTube `/youtubei/v1/browse` with `browseId=channel_id` and `params=CHANNEL_TAB_VIDEOS` (base64-decoded).
- Navigates to the "Videos" tab; falls back to "Home" tab if "Videos" is absent.
- Paginates via `onResponseReceivedCommands` / `onResponseReceivedActions` continuation.
- Raises `YouTubeStructureChangedError` if `twoColumnBrowseResultsRenderer.tabs` is missing.

---

### `channel_info.py` — Channel metadata

Entry point: `get_channel_info(channel_id, proxy)`

POSTs to InnerTube browse with `browseId`. Parses `pageHeaderRenderer` (handle, subscribers) and `channelMetadataRenderer` (name, description, avatar). Banner extracted from `imageBannerViewModel`. Returns: `channel_id`, `channel_name`, `handle`, `avatar`, `banner`, `subscriber_count`, `description`.

---

### `playlist.py` — Playlists and playlist videos

Two entry points:

- `get_playlist_videos(channel_id, proxy)` — fetches the channel's "Playlists" tab via up to three sequential browse requests (channel browse → Videos tab endpoint → Playlists tab endpoint). Parses `lockupViewModel` for title, thumbnail, and video count.
- `get_videos_from_playlist(playlist_id, proxy)` — browses `VL{playlist_id}`, paginates via continuation, extracts `playlistVideoRenderer` items. Raises `YouTubeStructureChangedError` if top-level `contents` is missing.
