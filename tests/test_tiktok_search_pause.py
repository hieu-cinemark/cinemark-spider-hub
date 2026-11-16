from social_crawler.spiders.tiktok import search_pause as sp


class _Cache:
    def __init__(self):
        self.store = {}

    def exists(self, key):
        return key in self.store

    def set(self, key, value, ttl_seconds=None):
        self.store[key] = value

    def add_if_new(self, key, ttl):
        if key in self.store:
            return False
        self.store[key] = 1
        return True

    def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    def expire(self, key, ttl):
        return None

    def delete(self, key):
        self.store.pop(key, None)


def test_zero_results_pause_only_after_streak(monkeypatch):
    cache = _Cache()
    monkeypatch.setattr(sp, "RedisCache", lambda: cache)
    assert not sp.note_page_one(0, cache)  # một từ khoá rỗng có thể là thật
    assert not sp.is_paused(cache)
    assert sp.note_page_one(0, cache)  # từ khoá thứ hai liên tiếp rỗng -> nghỉ
    assert sp.is_paused(cache)


def test_results_reset_streak(monkeypatch):
    cache = _Cache()
    monkeypatch.setattr(sp, "RedisCache", lambda: cache)
    sp.note_page_one(0, cache)
    sp.note_page_one(12, cache)
    assert not sp.note_page_one(0, cache)
    assert not sp.is_paused(cache)


def test_min_gap_between_searches():
    cache = _Cache()
    assert sp.claim_slot(cache)
    assert not sp.claim_slot(cache)
