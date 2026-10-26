"""clients/ai.classify_hashtag_relevance: không điền decisions_path -> dùng model chat (Haiku), không cảnh báo."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from social_crawler.clients import ai


def test_no_decisions_path_uses_chat_without_warning(monkeypatch) -> None:
    warnings: list[str] = []
    monkeypatch.setattr(ai, "ai_is_enabled", lambda: True)
    monkeypatch.setattr(ai, "_cooldown_until", 0.0)
    monkeypatch.setattr(ai, "_decisions_unsupported_until", 0.0)
    monkeypatch.setattr(ai, "_load_ai_runtime", lambda: {"raw_base_url": "https://bee", "api_key": "k", "decisions_path": ""})
    monkeypatch.setattr(ai, "_complete_sync", lambda **_kw: SimpleNamespace(ok=True, content="relevant"))
    monkeypatch.setattr(ai.logger, "warning", lambda event, **_kw: warnings.append(event))

    assert asyncio.run(ai.classify_hashtag_relevance("traibuonnguoi", "reviewphimhay")) is True
    assert "ai_decisions_unsupported" not in warnings
    assert ai._decisions_unsupported_until == 0.0  # vẫn hỏi lại cấu hình ở lần sau (bật model quyết định là có tác dụng ngay)
