from dataclasses import dataclass


@dataclass
class ThreadsPostItem:
    query: str
    post_id: str | None = None
    code: str | None = None
    url: str | None = None
    message: str | None = None
    timestamp: int | None = None
    author_name: str | None = None
    author_username: str | None = None
    author_id: str | None = None
    author_url: str | None = None
    topic: str | None = None
    is_reply: bool | None = None
    like_count: int | None = None
    reply_count: int | None = None
    repost_count: int | None = None
    quote_count: int | None = None
    media_type: int | None = None
    media_url: str | None = None
