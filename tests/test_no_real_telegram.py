import requests

from social_crawler.clients import telegram


def test_tests_never_send_real_telegram(monkeypatch):
    """Guard của conftest (_no_real_telegram): dù `.env` / môi trường có token, test không gửi được tin thật."""

    def boom(*args, **kwargs):
        raise AssertionError("test gửi Telegram thật")

    monkeypatch.setattr(requests, "post", boom)
    assert not telegram.telegram_enabled()
    telegram.send_telegram_message("cảnh báo thử")  # không raise = không gọi requests.post
