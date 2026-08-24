from dataclasses import dataclass


@dataclass
class FacebookPostItem:
    query: str
    post_id: str | None = None
    url: str | None = None
    message: str | None = None
    timestamp: int | None = None
    author_name: str | None = None
    author_id: str | None = None
    author_url: str | None = None
    comments_count: int | None = None
    reactions_count: int | None = None
    reactions: dict[str, int] | None = None
    shares_count: int | None = None
    hashtags: list[str] | None = None
    media_type: str | None = None
    media_url: str | None = None
    duration_seconds: float | None = None


@dataclass
class FacebookEntityItem:
    query: str
    type: str | None = None
    id: str | None = None
    name: str | None = None
    url: str | None = None


@dataclass
class FacebookCommentItem:
    post_id: str
    comment_id: str | None = None
    legacy_comment_id: str | None = None
    message: str | None = None
    date: str | None = None
    timestamp: int | None = None
    author_name: str | None = None
    author_id: str | None = None
    author_url: str | None = None
    author_gender: str | None = None
    author_profile_picture: str | None = None
    replies_count: int | None = None
    reactions_count: int | None = None
    is_sticker: bool | None = None
    sticker_url: str | None = None
    is_gif: bool | None = None
    gif: str | None = None
    image: str | None = None
    video: str | None = None
