"""Poll khi đang tạm dừng (giờ yên lặng, trần theo giờ, hàng chờ public) không được làm mất message (18/11/2026).

Rebalance giữa lúc pause giao partition mới chưa bị pause; getmany trả message của nó. Trước đây message đó bị bỏ đi và lần
commit() kế tiếp đẩy offset qua - job mất hẳn.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from social_crawler.consumer import loop


class FakeConsumer:
    """Mô phỏng aiokafka: pause theo partition, rebalance có thể thêm partition chưa pause."""

    def __init__(self, assigned, batches):
        self.assigned = set(assigned)
        self.paused: set = set()
        self.batches = list(batches)
        self.seeks: list[tuple] = []
        self.resumed: set = set()

    def assignment(self):
        return set(self.assigned)

    def pause(self, *partitions):
        self.paused.update(partitions)

    def resume(self, *partitions):
        self.resumed.update(partitions)
        self.paused.difference_update(partitions)

    def seek(self, partition, offset):
        self.seeks.append((partition, offset))

    async def getmany(self, timeout_ms=0, max_records=None):
        batch = self.batches.pop(0) if self.batches else {}
        # Chỉ partition chưa pause mới trả được message - giống broker thật.
        return {p: msgs for p, msgs in batch.items() if p not in self.paused}


def _msg(offset):
    return SimpleNamespace(offset=offset, value={"platform": "facebook"})


def test_poll_paused_pauses_new_partitions_before_polling():
    consumer = FakeConsumer(assigned={"p0"}, batches=[{"p1": [_msg(40)]}])
    consumer.assigned.add("p1")  # rebalance vừa giao p1, chưa ai pause nó

    asyncio.run(loop._poll_paused(consumer, 10))

    assert "p1" in consumer.paused
    assert consumer.seeks == []  # đã pause trước khi poll nên không đọc lỡ gì


def test_poll_paused_seeks_back_anything_it_did_read():
    consumer = FakeConsumer(assigned={"p0"}, batches=[{"p2": [_msg(7), _msg(8)]}])

    async def run():
        # p2 được giao NGAY trong lúc getmany chạy (sau lúc pause) - message đọc được phải được trả về đúng offset đầu.
        original = consumer.getmany

        async def racing_getmany(timeout_ms=0, max_records=None):
            consumer.assigned.add("p2")
            return await original(timeout_ms)

        consumer.getmany = racing_getmany
        await loop._poll_paused(consumer, 10)

    asyncio.run(run())
    assert consumer.seeks == [("p2", 7)]


def test_quiet_hours_wait_does_not_drop_messages(monkeypatch):
    consumer = FakeConsumer(assigned={"p0"}, batches=[{"p1": [_msg(12)]}])
    consumer.assigned.add("p1")
    left = iter([3600.0, 3600.0, 0.0])
    monkeypatch.setattr(loop, "_quiet_seconds_left", lambda platform: next(left))
    monkeypatch.setattr(loop, "is_platform_draining", lambda platform: False)

    async def no_idle_job(platform):
        return False

    monkeypatch.setattr(loop, "_run_one_idle_job", no_idle_job)

    asyncio.run(loop._wait_out_quiet_hours(consumer, "facebook"))

    # Không message nào bị nuốt: hoặc chưa đọc (đã pause), hoặc đã seek về.
    assert all(offset == 12 for _p, offset in consumer.seeks)
    assert consumer.resumed == {"p0", "p1"}  # hết giờ yên lặng thì mở lại cả partition mới nhận
