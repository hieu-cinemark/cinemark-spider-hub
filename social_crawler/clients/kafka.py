from __future__ import annotations

import os
from typing import Any

import orjson
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
        # Đếm số lần gọi publish() bị bỏ âm thầm vì start() không bao giờ có được producer hoạt
        # động - không có cái này, một lượt crawl với kết nối Kafka đã chết vẫn chạy xong "thành
        # công" trong khi mọi item nó tìm được đều bị âm thầm vứt bỏ, mỗi item một dòng log giống
        # hệt nhau không có ngữ cảnh. Xem publish() bên dưới.
        self._dropped_count = 0

    async def start(self) -> None:
        producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=orjson.dumps,
            key_serializer=lambda k: k.encode("utf-8"),
            linger_ms=50,
        )
        try:
            await producer.start()
        except KafkaConnectionError as exc:
            logger.error(
                "kafka_connection_error", telegram=True, bootstrap_servers=self.bootstrap_servers, error=str(exc)
            )
            await producer.stop()
            return
        self._producer = producer

    async def publish(self, topic: str, key: str, value: dict[str, Any]) -> bool:
        """True khi broker đã nhận message. False (có log) khi producer chưa khởi động, mất kết
        nối hoặc gửi lỗi vì bất kỳ lý do nào khác - chỗ gọi dùng nó để không đánh dấu "đã thấy"
        một item chưa tới ingest."""
        if not self._producer:
            self._dropped_count += 1
            logger.error(
                "kafka_producer_not_started",
                # Chỉ lần bỏ đầu tiên của mỗi instance mới báo động - các item còn lại của cùng lượt
                # crawl cũng sẽ gặp lỗi này, và cảnh báo theo từng item chỉ spam cùng một nguyên nhân gốc.
                telegram=self._dropped_count == 1,
                topic=topic,
                key=key,
                dropped_count=self._dropped_count,
            )
            return False
        try:
            await self._producer.send_and_wait(topic, key=key, value=value)
        except KafkaConnectionError as exc:
            logger.error("kafka_connection_error", topic=topic, key=key, error=str(exc))
            return False
        except Exception as exc:  # noqa: BLE001 - lỗi serialize/message quá lớn/...: trả False thay vì làm sập cả lượt crawl
            # Ném lỗi ra ngoài (như lần value_serializer sai 2026-10-06) dừng cả spider giữa chừng và để lại
            # dấu "đã thấy" của item đang xử lý, nên nó bị bỏ qua tới hết TTL. Trả False để chỗ gọi xoá dấu đó.
            self._dropped_count += 1
            logger.error(
                "kafka_publish_failed",
                telegram=self._dropped_count == 1,
                topic=topic,
                key=key,
                error_type=type(exc).__name__,
                error=str(exc),
                dropped_count=self._dropped_count,
            )
            return False
        return True

    async def stop(self) -> None:
        if not self._producer:
            return
        try:
            await self._producer.stop()
        except KafkaConnectionError as exc:
            logger.error("kafka_connection_error", error=str(exc))
