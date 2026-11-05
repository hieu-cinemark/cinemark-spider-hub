"""Kiểm tra trước khi chạy Facebook trên Mac: Kafka và Redis qua tunnel đúng là của VPS. Dừng nếu không chắc: consumer nối nhầm
Kafka local sẽ không nhận job nào, còn bài cào được sẽ không về ingest trên VPS."""

from __future__ import annotations

import asyncio
import os
import sys

from aiokafka import AIOKafkaConsumer

from social_crawler.clients.kafka import CRAWL_REQUESTS_TOPIC
from social_crawler.clients.redis import RedisCache

# VPS có 4 partition, Kafka local có 1.
VPS_CRAWL_PARTITIONS = 4
MIN_VPS_REDIS_KEYS = 50_000


async def _partitions() -> int:
    consumer = AIOKafkaConsumer(CRAWL_REQUESTS_TOPIC, bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"], group_id=None)
    await asyncio.wait_for(consumer.start(), timeout=20)
    try:
        return len(consumer.partitions_for_topic(CRAWL_REQUESTS_TOPIC) or ())
    finally:
        await consumer.stop()


def main() -> int:
    partitions = asyncio.run(_partitions())
    keys = RedisCache()._client.dbsize()
    print(f"kafka {CRAWL_REQUESTS_TOPIC} partitions={partitions}, redis keys={keys}")
    if partitions != VPS_CRAWL_PARTITIONS or keys < MIN_VPS_REDIS_KEYS:
        print("Không giống Kafka/Redis của VPS, dừng lại.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
