"""Hỏi "phiên Facebook còn trong cache không" không được tự gia hạn phiên (18/11/2026) - gia hạn là việc riêng của
_keep_recipes_alive, gọi ở đầu ensure_facebook_session."""

import time

from social_crawler.consumer import sessions


class _Cache:
    def __init__(self, data):
        self.data = dict(data)
        self.sets: list[str] = []

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self.sets.append(key)
        self.data[key] = value

    def delete(self, key):
        self.data.pop(key, None)


def _session(age_seconds: float) -> dict:
    return {
        "fb_api_req_friendly_name": "SearchCometResultsPaginatedResultsQuery",
        "captured_at": time.time() - age_seconds,
    }


def test_checking_the_session_does_not_extend_it(monkeypatch):
    key = sessions.CACHE_REDIS_KEY_TMPL.format(account="acc")
    cache = _Cache({key: _session(60)})
    monkeypatch.setattr(sessions, "RedisCache", lambda: cache)
    monkeypatch.setattr(sessions, "is_search_recipe", lambda session: True)

    assert sessions.facebook_session_is_cached("acc") is True
    assert cache.sets == []


def test_keep_recipes_alive_extends_search_comment_and_reply_recipes(monkeypatch):
    search = sessions.CACHE_REDIS_KEY_TMPL.format(account="acc")
    comments = sessions.COMMENTS_REDIS_KEY_TMPL.format(account="acc")
    cache = _Cache({search: _session(10**6), comments: {"x": 1}})  # đã quá hạn tuổi nhưng vẫn phải còn cho bước làm mới
    monkeypatch.setattr(sessions, "RedisCache", lambda: cache)
    monkeypatch.setattr(sessions, "is_search_recipe", lambda session: True)

    sessions._keep_recipes_alive("acc")

    assert set(cache.sets) == {search, comments}  # reply chưa có thì không tạo ra
