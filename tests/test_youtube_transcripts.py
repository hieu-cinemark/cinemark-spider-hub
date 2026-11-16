import asyncio

from social_crawler.consumer.jobs import youtube as job


class _Cache:
    def __init__(self):
        self.keys: set[str] = set()

    def add_if_new(self, key, ttl_seconds):
        if key in self.keys:
            return False
        self.keys.add(key)
        return True

    def delete(self, key):
        self.keys.discard(key)

    def exists(self, key):
        return key in self.keys

    def set(self, key, value, ttl_seconds=None):
        self.keys.add(key)


class _Kafka:
    def __init__(self):
        self.values = []

    async def publish(self, topic, key, value):
        self.values.append(value)
        return True


def _setup(monkeypatch, transcripts, switch=True, lane="http://lane"):
    calls = []
    job._transcript_exit_rest.clear()

    async def fake_transcript(video_id, proxy=None):
        calls.append(video_id)
        result = transcripts[video_id]
        if callable(result):
            result = result(proxy)
        if isinstance(result, Exception):
            raise result
        return result

    async def fake_meta(video_id, proxy=None):
        return {"video_id": video_id, "title": f"Video {video_id}"}

    async def no_proxy():
        return lane

    monkeypatch.setattr(job, "get_transcript", fake_transcript)
    monkeypatch.setattr(job, "get_video_meta", fake_meta)
    monkeypatch.setattr(job, "lane_proxy", no_proxy)
    monkeypatch.setattr(job, "get_setting", lambda key: {"youtube_transcripts": switch}[key])
    monkeypatch.setattr(job, "log_crawled_post", lambda *a, **k: None)
    return calls


TEXT = {"language": "vi", "generated": True, "text": "Con Mắt Thứ Ba chính thức tung trailer", "char_count": 38}


def test_transcript_sent_with_video_and_fetched_once(monkeypatch):
    calls = _setup(monkeypatch, {"a": TEXT, "b": None})
    cache, kafka = _Cache(), _Kafka()
    sent = asyncio.run(job._publish_videos(cache, kafka, [{"video_id": "a"}, {"video_id": "b"}], "k1"))
    assert sent == 2
    by_id = {v["video_id"]: v for v in kafka.values}
    assert by_id["a"]["transcript"] == TEXT
    assert "transcript" not in by_id["b"]  # video không có phụ đề
    # Hết TTL "đã thấy", video được gửi lại để cập nhật view/like nhưng không lấy transcript lần nữa.
    cache.keys = {k for k in cache.keys if k.startswith(job.TRANSCRIPT_DONE_KEY)}
    asyncio.run(job._publish_videos(cache, kafka, [{"video_id": "a"}], "k1"))
    assert calls == ["a", "b"]
    assert "transcript" not in kafka.values[-1]


def test_failed_fetch_is_retried_later(monkeypatch):
    calls = _setup(monkeypatch, {"a": RuntimeError("429 Too Many Requests")})
    cache, kafka = _Cache(), _Kafka()
    assert asyncio.run(job._publish_videos(cache, kafka, [{"video_id": "a"}], None)) == 1
    assert f"{job.TRANSCRIPT_DONE_KEY}:a" not in cache.keys
    assert "transcript" not in kafka.values[0]
    cache.keys.clear()
    asyncio.run(job._publish_videos(cache, kafka, [{"video_id": "a"}], None))
    assert calls == ["a", "a"]


def test_switch_off_skips_transcripts(monkeypatch):
    calls = _setup(monkeypatch, {"a": TEXT}, switch=False)
    kafka = _Kafka()
    asyncio.run(job._publish_videos(_Cache(), kafka, [{"video_id": "a"}], None))
    assert calls == []
    assert "transcript" not in kafka.values[0]


def test_blocked_proxy_falls_back_to_direct(monkeypatch):
    from social_crawler.spiders.youtube.transcript.transcript import TranscriptBlocked

    def by_exit(proxy):
        return TranscriptBlocked("IpBlocked") if proxy else TEXT

    _setup(monkeypatch, {"a": by_exit, "b": by_exit})
    kafka = _Kafka()
    asyncio.run(job._publish_videos(_Cache(), kafka, [{"video_id": "a"}, {"video_id": "b"}], None))
    assert all(v.get("transcript") == TEXT for v in kafka.values)
    assert "http://lane" in job._transcript_exit_rest  # proxy nghỉ, video sau đi thẳng


def test_every_exit_blocked_pauses_transcripts(monkeypatch):
    from social_crawler.spiders.youtube.transcript.transcript import TranscriptBlocked

    calls = _setup(monkeypatch, {"a": TranscriptBlocked("IpBlocked"), "b": TEXT})
    cache, kafka = _Cache(), _Kafka()
    asyncio.run(job._publish_videos(cache, kafka, [{"video_id": "a"}], None))
    assert job.TRANSCRIPT_PAUSE_KEY in cache.keys
    assert f"{job.TRANSCRIPT_DONE_KEY}:a" not in cache.keys  # lấy lại sau khi hết nghỉ
    asyncio.run(job._publish_videos(cache, kafka, [{"video_id": "b"}], None))
    assert "b" not in calls and "transcript" not in kafka.values[-1]  # đang nghỉ: không gọi YouTube
