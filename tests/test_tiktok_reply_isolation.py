import asyncio

from social_crawler.spiders.tiktok.client import TikTokBlockedError
from social_crawler.spiders.tiktok.features.comments import comments as mod


class _Client:
    """comment/list chạy bình thường, comment/list/reply luôn trả body rỗng (bị chặn)."""

    made = 0

    def __init__(self, *args, **kwargs):
        type(self).made += 1
        self._device_id = "d"

    def warm_session(self):
        return None

    def list_comments(self, video_id, cursor=0, count=50, video_url=""):
        return {
            "status_code": 0,
            "has_more": 0,
            "comments": [{"comment_id": f"c{i}", "reply_count": 2} for i in range(5)],
        }

    def list_replies(self, **kwargs):
        raise TikTokBlockedError("TikTok returned an empty response (status=200)")


class _Kafka:
    async def publish(self, **kwargs):
        return True


def _spider(monkeypatch):
    _Client.made = 0
    monkeypatch.setattr(mod, "TikTokCommentClient", _Client)
    monkeypatch.setattr(mod, "extract_comments", lambda data, parent_comment_id=None: data.get("comments", []))
    monkeypatch.setattr(mod, "TikTokCommentItem", lambda **kw: kw)
    spider = mod.TikTokCommentsSpider(video_id="v1", video_url="https://www.tiktok.com/@a/video/v1", dedupe="false")
    spider._kafka = _Kafka()
    return spider


async def _collect(spider):
    return [item async for item in spider._crawl_with_fresh_identity()]


def test_blocked_replies_keep_top_level_comments_and_stop_after_limit(monkeypatch):
    spider = _spider(monkeypatch)
    items = asyncio.run(_collect(spider))
    assert [i["comment_id"] for i in items] == [f"c{i}" for i in range(5)]  # không làm lại cả video, không raise
    assert spider._reply_blocks == mod.REPLY_BLOCK_LIMIT
    assert spider._replies_skipped == 5  # 3 bị chặn + 2 bỏ qua sau khi chạm trần
    # 1 danh tính cho video + 1 danh tính mới cho mỗi comment bị chặn (chỉ tới khi chạm trần)
    assert _Client.made == 1 + mod.REPLY_BLOCK_LIMIT
