"""KafkaPublisher.publish (social_crawler/clients/kafka.py): mọi lỗi gửi đều trả False thay vì ném ra,
để chỗ gọi gỡ dấu "đã thấy" và crawl tiếp - lần value_serializer sai 2026-10-06 từng làm sập cả spider."""

from __future__ import annotations

import asyncio

import orjson
from aiokafka.errors import KafkaConnectionError

from social_crawler.clients.kafka import KafkaPublisher


class _FakeProducer:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.sent: list[tuple[str, str, bytes]] = []

    async def send_and_wait(self, topic: str, key: str, value: dict) -> None:
        if self.error:
            raise self.error
        self.sent.append((topic, key, orjson.dumps(value)))


def _publisher(producer: _FakeProducer | None) -> KafkaPublisher:
    publisher = KafkaPublisher(bootstrap_servers="localhost:0")
    publisher._producer = producer  # bỏ qua start(): không cần broker thật
    return publisher


def _publish(publisher: KafkaPublisher) -> bool:
    return asyncio.run(publisher.publish("raw_comments", "tiktok:1", {"platform": "tiktok", "text": "Trại Buôn Người 🐑"}))


def test_publish_ok() -> None:
    producer = _FakeProducer()
    assert _publish(_publisher(producer)) is True
    assert orjson.loads(producer.sent[0][2]) == {"platform": "tiktok", "text": "Trại Buôn Người 🐑"}


def test_serializer_bug_returns_false_instead_of_raising() -> None:
    error = TypeError("a bytes-like object is required, not 'builtin_function_or_method'")
    assert _publish(_publisher(_FakeProducer(error))) is False


def test_connection_error_returns_false() -> None:
    assert _publish(_publisher(_FakeProducer(KafkaConnectionError("down")))) is False


def test_not_started_returns_false() -> None:
    assert _publish(_publisher(None)) is False
