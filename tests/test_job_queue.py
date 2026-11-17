"""JobQueue: hàng đợi tin cậy dùng chung (claim -> ack, requeue khi chưa chạy được, recover sau khi tiến trình chết)."""

import json

from social_crawler.runtime.job_queue import JobQueue


class FakeRedisCache:
    """Giả lập đúng ngữ nghĩa RedisCache: claim = LMOVE từ phải sang đầu processing; requeue_claim chỉ trả đúng mục."""

    def __init__(self):
        self.lists: dict[str, list[str]] = {}

    def _l(self, key):
        return self.lists.setdefault(key, [])

    def push(self, key, value):
        self._l(key).insert(0, json.dumps(value))

    def claim_json(self, key, processing):
        if not self._l(key):
            return None, None
        raw = self._l(key).pop()
        self._l(processing).insert(0, raw)
        return raw, json.loads(raw)

    def ack(self, processing, raw):
        self._l(processing).remove(raw)

    def requeue_claim(self, processing, key, raw):
        if raw not in self._l(processing):
            return False
        self._l(processing).remove(raw)
        self._l(key).insert(0, raw)
        return True

    def requeue_processing(self, processing, key):
        moved = 0
        while self._l(processing):
            self._l(key).append(self._l(processing).pop())
            moved += 1
        return moved


def test_processing_key_defaults_to_suffix_of_key():
    assert JobQueue("crawl_idle:facebook").processing == "crawl_idle:facebook:processing"
    assert JobQueue("a", "b").processing == "b"


def test_claim_ack_cycle_is_first_in_first_out():
    cache, queue = FakeRedisCache(), JobQueue("q")
    cache.push("q", {"n": 1})
    cache.push("q", {"n": 2})
    raw, first = queue.claim(cache)
    assert first == {"n": 1} and cache.lists["q:processing"] == [raw]
    queue.ack(cache, raw)
    assert cache.lists["q:processing"] == []
    assert queue.claim(cache)[1] == {"n": 2}
    assert queue.claim(cache) == (None, None) or queue.claim(cache)[0] is None


def test_requeue_puts_the_claimed_job_back_and_ignores_unknown_ones():
    cache, queue = FakeRedisCache(), JobQueue("q")
    cache.push("q", {"n": 1})
    raw, _ = queue.claim(cache)
    assert queue.requeue(cache, raw) is True
    assert cache.lists["q:processing"] == [] and len(cache.lists["q"]) == 1
    assert queue.requeue(cache, raw) is False  # đã trả rồi, không nhân đôi


def test_recover_returns_jobs_left_by_a_dead_process():
    cache, queue = FakeRedisCache(), JobQueue("q")
    for n in range(3):
        cache.push("q", {"n": n})
    for _ in range(3):
        queue.claim(cache)  # tiến trình chết trước khi ack
    assert cache.lists["q"] == []
    assert queue.recover(cache) == 3
    assert len(cache.lists["q"]) == 3 and cache.lists["q:processing"] == []


def test_requeue_to_another_destination():
    cache, queue = FakeRedisCache(), JobQueue("search", "proc")
    cache.push("search", {"type": "comments"})
    raw, _ = queue.claim(cache)
    assert queue.requeue(cache, raw, destination="comments") is True
    assert len(cache.lists["comments"]) == 1
