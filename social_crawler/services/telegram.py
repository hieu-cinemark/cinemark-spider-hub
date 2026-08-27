from __future__ import annotations

import requests

from social_crawler.logger import get_logger
from social_crawler.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = get_logger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


def telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram_message(text: str) -> None:
    if not telegram_enabled():
        return
    try:
        resp = requests.post(
            TELEGRAM_API_URL.format(token=TELEGRAM_BOT_TOKEN),
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4096]},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning("telegram_send_failed", status_code=resp.status_code, body=resp.text[:300])
    except requests.RequestException as exc:
        logger.warning("telegram_send_failed", error=str(exc))
