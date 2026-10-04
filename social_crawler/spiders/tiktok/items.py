from dataclasses import dataclass


@dataclass
class TikTokVideoItem:
    hashtag: str
    video_id: str | None = None
    url: str | None = None
    desc: str | None = None
    # Phỏng đoán ngôn ngữ caption và quốc gia đăng của TikTok - xem extract_video trong
    # hashtag_search/extract.py.
    text_language: str | None = None
    location_created: str | None = None
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
    """Cùng các trường như TikTokVideoItem, nhưng theo username kênh/creator có feed video đã đăng
    mà item này đến từ - xem features/channel_videos/search.py. username là handle không có '@'
    (khớp với tham số `username` của spider đó)."""

    username: str
    video_id: str | None = None
    url: str | None = None
    desc: str | None = None
    # Phỏng đoán ngôn ngữ caption và quốc gia đăng của TikTok - xem extract_video trong
    # hashtag_search/extract.py.
    text_language: str | None = None
    location_created: str | None = None
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
    # Đặt cho các dòng /api/comment/list/reply/ — id comment cấp một cha.
    parent_comment_id: str | None = None
