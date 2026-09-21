from dataclasses import dataclass


@dataclass
class TikTokVideoItem:
    hashtag: str
    video_id: str | None = None
    url: str | None = None
    desc: str | None = None
    create_time: int | None = None
    author_id: str | None = None
    author_username: str | None = None
    author_name: str | None = None
    author_avatar_url: str | None = None
    duration: int | None = None
    cover_url: str | None = None
    play_url: str | None = None
    music_title: str | None = None
    hashtags: list[str] | None = None
    play_count: int | None = None
    like_count: int | None = None
    comment_count: int | None = None
    share_count: int | None = None
    collect_count: int | None = None


@dataclass
class TikTokChannelVideoItem:
    """Same fields as TikTokVideoItem, but keyed by the channel/creator
    username whose posted-videos feed this came from - see
    features/channel_videos/search.py. username is the handle without '@'
    (matches that spider's own `username` argument)."""

    username: str
    video_id: str | None = None
    url: str | None = None
    desc: str | None = None
    create_time: int | None = None
    author_id: str | None = None
    author_username: str | None = None
    author_name: str | None = None
    author_avatar_url: str | None = None
    duration: int | None = None
    cover_url: str | None = None
    play_url: str | None = None
    music_title: str | None = None
    hashtags: list[str] | None = None
    play_count: int | None = None
    like_count: int | None = None
    comment_count: int | None = None
    share_count: int | None = None
    collect_count: int | None = None


@dataclass
class TikTokCommentItem:
    video_id: str
    comment_id: str | None = None
    message: str | None = None
    timestamp: int | None = None
    like_count: int | None = None
    reply_count: int | None = None
    author_id: str | None = None
    author_username: str | None = None
    author_name: str | None = None
    author_avatar_url: str | None = None
    # Set for /api/comment/list/reply/ rows — parent top-level comment id.
    parent_comment_id: str | None = None
