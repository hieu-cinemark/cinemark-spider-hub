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
CRAWL_REQUESTS_TOPIC = "crawl_requests"


class KafkaPublisher:
    def __init__(self, bootstrap_servers: str | None = None):
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self._producer: AIOKafkaProducer | None = None
        # Counts publish() calls that were silently dropped because start()
        # never got a working producer - without this, a crawl with a dead
        # Kafka connection still runs to completion "successfully" while
        # every single item it found is quietly discarded, one identical
        # context-free log line per item. See publish() below.
        self._dropped_count = 0

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
            logger.error("kafka_connection_error", telegram=True, bootstrap_servers=self.bootstrap_servers, error=str(exc))
            await producer.stop()
            return
        self._producer = producer

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        if not self._producer:
            self._dropped_count += 1
            logger.error(
                "kafka_producer_not_started",
                # Only the first drop per instance pages - the same crawl's
                # remaining items will keep hitting this too, and alerting
                # once per item would just spam the same root cause.
                telegram=self._dropped_count == 1,
                topic=topic,
                key=key,
                dropped_count=self._dropped_count,
            )
            return
        try:
            await self._producer.send_and_wait(topic, key=key, value=value)
        except KafkaConnectionError as exc:
            logger.error("kafka_connection_error", topic=topic, key=key, error=str(exc))

    async def stop(self) -> None:
        if not self._producer:
            return
        try:
            await self._producer.stop()
        except KafkaConnectionError as exc:
            logger.error("kafka_connection_error", error=str(exc))
