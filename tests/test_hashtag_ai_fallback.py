"""clients/ai.classify_hashtag_relevance: hỏi chat (nhà cung cấp AI đang chọn), tên phim khớp thì khỏi hỏi."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from social_crawler.clients import ai


def test_hashtag_relevance_uses_chat(monkeypatch) -> None:
    monkeypatch.setattr(ai, "ai_is_enabled", lambda: True)
    monkeypatch.setattr(ai, "_cooldown_until", 0.0)
    monkeypatch.setattr(ai, "_complete_sync", lambda **_kw: SimpleNamespace(ok=True, content="relevant"))

    assert asyncio.run(ai.classify_hashtag_relevance("traibuonnguoi", "reviewphimhay")) is True


def test_hashtag_generic_answer(monkeypatch) -> None:
    monkeypatch.setattr(ai, "ai_is_enabled", lambda: True)
    monkeypatch.setattr(ai, "_cooldown_until", 0.0)
    monkeypatch.setattr(ai, "_complete_sync", lambda **_kw: SimpleNamespace(ok=True, content="generic"))

    assert asyncio.run(ai.classify_hashtag_relevance("traibuonnguoi", "xuhuong")) is False
