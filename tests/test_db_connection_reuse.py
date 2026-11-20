"""db/connection.py: connection được tái dùng giữa các lượt `with connect()`, commit/rollback đúng, hỏng thì bỏ."""

import psycopg
import pytest

from social_crawler.db import connection


class _Info:
    transaction_status = psycopg.pq.TransactionStatus.IDLE


class _Conn:
    def __init__(self):
        self.closed = False
        self.info = _Info()
        self.commits = self.rollbacks = 0
        self.fail_commit = False

    def commit(self):
        if self.fail_commit:
            raise psycopg.OperationalError("lost")
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


@pytest.fixture
def opened(monkeypatch):
    made: list[_Conn] = []

    def fake_open():
        made.append(_Conn())
        return made[-1]

    monkeypatch.setattr(connection, "_open", fake_open)
    monkeypatch.setattr(connection, "_idle", [])
    monkeypatch.setattr(connection, "_MAX_IDLE", 2)
    return made


def test_connection_reused_across_blocks(opened):
    for _ in range(5):
        with connection.connect() as conn:
            assert conn is opened[0]
    assert len(opened) == 1 and opened[0].commits == 5 and not opened[0].closed


def test_exception_rolls_back_and_keeps_connection(opened):
    with pytest.raises(ValueError), connection.connect():
        raise ValueError("boom")
    assert opened[0].rollbacks == 1 and opened[0].commits == 0 and not opened[0].closed


def test_failed_commit_discards_connection_and_raises(opened):
    with connection.connect():
        pass
    first = opened[0]
    first.fail_commit = True
    with pytest.raises(psycopg.OperationalError), connection.connect():
        pass
    assert first.closed
    with connection.connect() as conn:
        assert conn is not first


def test_expired_or_dirty_connection_not_reused(opened, monkeypatch):
    with connection.connect():
        pass
    at, conn = connection._idle[0]
    connection._idle[0] = (at - connection._IDLE_TTL_SECONDS - 1, conn)
    with connection.connect() as again:
        assert again is not conn
    assert conn.closed


def test_idle_cap(opened):
    leases = [connection.connect() for _ in range(4)]
    for lease in leases:
        lease.__enter__()
    for lease in leases:
        lease.__exit__(None, None, None)
    assert len(connection._idle) == 2
    assert sum(c.closed for c in opened) == 2


def test_disabled_falls_back_to_plain_connect(monkeypatch):
    monkeypatch.setattr(connection, "_MAX_IDLE", 0)
    called = {}
    monkeypatch.setattr(connection.psycopg, "connect", lambda *a, **k: called.setdefault("x", object()))
    assert connection.connect() is called["x"]
