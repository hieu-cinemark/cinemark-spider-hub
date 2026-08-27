from __future__ import annotations

import json
import os
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError

from social_crawler.logger import get_logger

logger = get_logger(__name__)

RAW_POSTS_TOPIC = "raw_posts"
RAW_COMMENTS_TOPIC = "raw_comments"


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str | None = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            linger_ms=50,
        )
        try:
            await producer.start()
        except KafkaConnectionError as exc:
            logger.error("kafka_connection_error", error=str(exc))
            await producer.stop()
            return
        self._producer = producer

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if not self._producer:
            logger.error("kafka_producer_not_started")
            return
        try:
            await self._producer.send_and_wait(topic, key=key, value=value)
        except KafkaConnectionError as exc:
            logger.error("kafka_connection_error", error=str(exc))

    async def stop(self) -> None:
        if not self._producer:
            return
        try:
            await self._producer.stop()
        except KafkaConnectionError as exc:
            logger.error("kafka_connection_error", error=str(exc))
