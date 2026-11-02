"""Kết quả có cấu trúc của một lượt `scrapy crawl` (02/11/2026): mã thoát 0 không có nghĩa lượt crawl thành công - exception
trong async start() của spider chỉ được engine log ERROR, spider vẫn đóng với lý do "finished" và lệnh scrapy vẫn thoát 0
(đã thử trên Scrapy 2.17). Mọi spider ở đây chạy vòng cào chính trong start(), nên đó là cả lượt crawl chết giữa chừng.

Extension này ghi kết quả ra file ở CRAWL_OUTCOME_FILE (consumer/process.py đặt cho mỗi tiến trình con) khi spider đóng;
không có biến môi trường đó (chạy tay) thì không làm gì. Trạng thái:
  - failed: start() lỗi, hoặc spider đóng với lý do bất thường;
  - cancelled: spider đóng vì tín hiệu dừng (nút Dừng / huỷ job);
  - partial: callback lỗi nhưng start() chạy hết, hoặc có item không publish được;
  - success: còn lại.
retryable=True khi có item không publish được: lỗi hạ tầng Kafka, dấu "đã thấy" của item đó đã được gỡ nên chạy lại lấy lại được.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from typing import Any

from scrapy import signals
from scrapy.exceptions import NotConfigured

from social_crawler.clients.kafka import publish_counts

OUTCOME_FILE_ENV = "CRAWL_OUTCOME_FILE"

SUCCESS = "success"
PARTIAL = "partial"
FAILED = "failed"
CANCELLED = "cancelled"

_ENGINE_LOGGER = "scrapy.core.engine"


class _EngineErrorCounter(logging.Handler):
    """Đếm dòng ERROR của engine - engine chỉ log lỗi start(), không có stat hay signal nào cho nó."""

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.count = 0

    def emit(self, record: logging.LogRecord) -> None:
        self.count += 1


def classify(
    *,
    finish_reason: str | None,
    engine_errors: int,
    spider_exceptions: int,
    published: int,
    publish_failed: int,
) -> dict[str, Any]:
    if finish_reason == "shutdown":
        status = CANCELLED
    elif engine_errors or finish_reason not in ("finished", "closespider_timeout", "closespider_itemcount"):
        status = FAILED
    elif spider_exceptions or publish_failed:
        status = PARTIAL
    else:
        status = SUCCESS
    return {
        "status": status,
        "retryable": status != CANCELLED and publish_failed > 0,
        "finish_reason": finish_reason,
        "engine_errors": engine_errors,
        "spider_exceptions": spider_exceptions,
        "published": published,
        "publish_failed": publish_failed,
    }


def _write_atomic(path: str, outcome: dict[str, Any]) -> None:
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path) or ".", prefix=".crawl-outcome-")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(outcome, handle)
    os.replace(tmp, path)


class CrawlOutcome:
    def __init__(self, crawler: Any, path: str) -> None:
        self._stats = crawler.stats
        self._path = path
        self._engine_errors = _EngineErrorCounter()
        logging.getLogger(_ENGINE_LOGGER).addHandler(self._engine_errors)
        crawler.signals.connect(self._spider_closed, signal=signals.spider_closed)

    @classmethod
    def from_crawler(cls, crawler: Any) -> CrawlOutcome:
        path = os.environ.get(OUTCOME_FILE_ENV)
        if not path:
            raise NotConfigured  # chạy `scrapy crawl` bằng tay
        return cls(crawler, path)

    def _spider_closed(self, spider: Any, reason: str) -> None:
        stats = self._stats.get_stats()
        outcome = classify(
            finish_reason=reason,
            engine_errors=self._engine_errors.count,
            spider_exceptions=sum(
                v for k, v in stats.items() if k.startswith("spider_exceptions/") and k.count("/") == 1
            ),
            published=publish_counts["ok"],
            publish_failed=publish_counts["failed"],
        )
        _write_atomic(self._path, outcome)
