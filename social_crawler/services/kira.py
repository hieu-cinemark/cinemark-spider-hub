"""Thin Kira (OpenAI-compatible LLM) client - sibling to cinemark-api's
app/kira/base.py. Same KiraResponse shape and log event names
(kira_call_started / kira_call_finished / kira_call_failed) so ingest and
crawl-side (Facebook/Threads/TikTok) traces line up.

Model + system prompts load from the shared ai_settings Postgres row
(dashboard Settings AI tab). Missing DB/credentials degrade to no-op.
"""

from __future__ import annotations

import asyncio
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any

import openai
from openai import OpenAI

from social_crawler import env  # noqa: F401 - import for its load_dotenv() side effect
from social_crawler.logger import get_logger

logger = get_logger(__name__)

_DEFAULT_MODEL = "qwen3.8-flash"
_KIRA_CONCURRENCY = asyncio.Semaphore(2)
_MAX_RATE_LIMIT_RETRIES = 3
_RETRY_BASE_SECONDS = 2.0
_PROMPT_LOG_CHARS = 2000
_CONTENT_LOG_CHARS = 4000

_HASHTAG_BFS_PROMPT = """
You are a hashtag relevance classifier for a TikTok content-discovery pipeline.

You are given a ROOT hashtag (what a crawler was already searching for,
usually a movie title or a closely related term) and a CANDIDATE hashtag
that co-occurred with it on multiple videos.

Decide whether the CANDIDATE is topically specific to the ROOT (the same
movie, franchise, cast, or a closely related term) or whether it is a
generic/unrelated tag that would just as likely co-occur with completely
unrelated content (e.g. "#fyp", "#xuhuong", "#reviewphim", "#hot", a
platform meme tag, or an unrelated movie/brand name).

Respond with ONLY one word, lowercase, no punctuation: "relevant" or "generic".
""".strip()

_DIAGNOSIS_SYSTEM_PROMPT = """
Bạn là trợ lý chẩn đoán sự cố cho một hệ thống tự động thu thập dữ liệu mạng xã hội
(đăng nhập và crawl Facebook/Threads/TikTok bằng tài khoản thật qua trình duyệt tự động).

Bạn sẽ nhận một mô tả lỗi kỹ thuật (tiếng Anh) tại thời điểm hệ thống vừa vô hiệu hoá
một tài khoản vì nghi ngờ bị checkpoint/chặn đăng nhập.

Trả lời bằng tiếng Việt, TỐI ĐA 2 câu ngắn gọn:
1) nguyên nhân nhiều khả năng nhất (ví dụ: sai proxy/vị trí đăng nhập, cookie hết hạn,
   2FA cần xác minh thủ công, mật khẩu sai, tài khoản bị Meta/TikTok khoá thật sự...)
2) người vận hành nên làm gì tiếp theo.

Không chào hỏi, không markdown, không nhắc lại nguyên văn lỗi - chỉ trả về đúng nội
dung chẩn đoán.
""".strip()

_SELECTOR_SYSTEM_PROMPT = """
You are a UI element picker for a browser-automation web scraper.

You are given a short GOAL describing what the automation is trying to
click on a real, currently-loaded web page, and a numbered list of
interactive elements actually present on that page right now (their
accessibility role, aria-label, and any visible text - never a CSS
selector or XPath, since you cannot see the page's real markup and must
never invent one).

Pick the ONE numbered entry whose role/label/text most plausibly matches
the GOAL. Respond with ONLY that number - no words, no punctuation, no
explanation.

If NONE of the entries plausibly match the GOAL at all, respond with
exactly: none
""".strip()

_CODE_DEFAULT_PROMPTS = {
    "hashtag_bfs": _HASHTAG_BFS_PROMPT,
    "diagnosis": _DIAGNOSIS_SYSTEM_PROMPT,
    "selector": _SELECTOR_SYSTEM_PROMPT,
}

_client: OpenAI | None = None
_client_model: str | None = None
_client_checked = False
_ai_cfg_cache: tuple[float, dict[str, Any]] | None = None
_AI_CFG_TTL_SECONDS = 5.0


def _preview(text: str, limit: int) -> str:
    value = text or ""
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...<{len(value) - limit} more chars>"


@dataclass
class KiraUsage:
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None

    def as_log_fields(self) -> dict[str, int | None]:
        return {
            "prompt_tokens": self.prompt_tokens,
            "completion_tokens": self.completion_tokens,
            "total_tokens": self.total_tokens,
        }


@dataclass
class KiraResponse:
    """Same fields as cinemark-api's app.kira.base.KiraResponse."""

    ok: bool
    task: str
    model: str
    content: str = ""
    finish_reason: str | None = None
    usage: KiraUsage | None = None
    latency_ms: int = 0
    error: str | None = None
    platform: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def as_log_fields(self) -> dict[str, Any]:
        fields: dict[str, Any] = {
            "ok": self.ok,
            "task": self.task,
            "model": self.model,
            "finish_reason": self.finish_reason,
            "latency_ms": self.latency_ms,
            "content_chars": len(self.content or ""),
            "content": _preview(self.content, _CONTENT_LOG_CHARS),
            "error": self.error,
            "platform": self.platform,
        }
        if self.usage is not None:
            fields.update(self.usage.as_log_fields())
        if self.extra:
            fields.update(self.extra)
        return fields


def _load_ai_runtime() -> dict[str, Any]:
    global _ai_cfg_cache
    now = time.monotonic()
    if _ai_cfg_cache is not None and now - _ai_cfg_cache[0] < _AI_CFG_TTL_SECONDS:
        return _ai_cfg_cache[1]
    cfg = {"enabled": False, "model": _DEFAULT_MODEL, "prompts": {}}
    try:
        from social_crawler.services.db import get_ai_settings

        cfg = get_ai_settings()
    except Exception as exc:
        logger.warning("ai_settings_load_failed", error=str(exc))
    _ai_cfg_cache = (now, cfg)
    return cfg


def kira_is_enabled() -> bool:
    return bool(_load_ai_runtime().get("enabled"))


def _resolve_prompt(task: str) -> str:
    stored = _load_ai_runtime().get("prompts") or {}
    custom = (stored.get(task) or "").strip()
    if custom:
        return custom
    return _CODE_DEFAULT_PROMPTS.get(task, "")


def _get_client() -> OpenAI | None:
    global _client, _client_checked, _client_model
    cfg = _load_ai_runtime()
    model = cfg.get("model") or _DEFAULT_MODEL
    if _client is not None and _client_model == model:
        return _client
    api_key = os.getenv("KIRA_API_KEY")
    base_url = os.getenv("KIRA_BASE_URL")
    _client_checked = True
    if not api_key or not base_url:
        _client = None
        _client_model = None
        return None
    _client = OpenAI(base_url=base_url, api_key=api_key)
    _client_model = model
    return _client


def _complete_sync(
    *,
    task: str,
    user_prompt: str,
    system_prompt: str,
    max_tokens: int,
    platform: str | None = None,
    extra: dict[str, Any] | None = None,
) -> KiraResponse:
    client = _get_client()
    model = _load_ai_runtime().get("model") or _DEFAULT_MODEL
    if client is None:
        result = KiraResponse(
            ok=False,
            task=task,
            model=model,
            error="kira_not_configured",
            platform=platform,
            extra=extra or {},
        )
        logger.warning("kira_call_failed", **result.as_log_fields())
        return result

    logger.info(
        "kira_call_started",
        task=task,
        model=model,
        platform=platform,
        max_tokens=max_tokens,
        system_prompt_chars=len(system_prompt),
        user_prompt_chars=len(user_prompt or ""),
        system_prompt=_preview(system_prompt, _PROMPT_LOG_CHARS),
        user_prompt=_preview(user_prompt or "", _PROMPT_LOG_CHARS),
    )
    started = time.perf_counter()
    try:
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.0,
            max_tokens=max_tokens,
        )
    except Exception as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        result = KiraResponse(
            ok=False,
            task=task,
            model=model,
            latency_ms=latency_ms,
            error=str(exc),
            platform=platform,
            extra=extra or {},
        )
        logger.error("kira_call_failed", **result.as_log_fields())
        raise

    latency_ms = int((time.perf_counter() - started) * 1000)
    choice = completion.choices[0] if completion.choices else None
    content = (choice.message.content if choice and choice.message else None) or ""
    finish_reason = getattr(choice, "finish_reason", None) if choice else None
    raw_usage = getattr(completion, "usage", None)
    usage = None
    if raw_usage is not None:
        usage = KiraUsage(
            prompt_tokens=getattr(raw_usage, "prompt_tokens", None),
            completion_tokens=getattr(raw_usage, "completion_tokens", None),
            total_tokens=getattr(raw_usage, "total_tokens", None),
        )
    result = KiraResponse(
        ok=bool(content.strip()),
        task=task,
        model=model,
        content=content,
        finish_reason=str(finish_reason) if finish_reason else None,
        usage=usage,
        latency_ms=latency_ms,
        platform=platform,
        extra=extra or {},
    )
    logger.info("kira_call_finished", **result.as_log_fields())
    return result


async def _complete_with_retry(**kwargs: Any) -> KiraResponse | None:
    if not kira_is_enabled():
        return None
    if _get_client() is None:
        logger.warning("kira_call_failed", task=kwargs.get("task"), error="kira_not_configured", ok=False)
        return None
    for attempt in range(1, _MAX_RATE_LIMIT_RETRIES + 1):
        try:
            return await asyncio.to_thread(lambda: _complete_sync(**kwargs))
        except openai.RateLimitError:
            if attempt == _MAX_RATE_LIMIT_RETRIES:
                logger.error("kira_call_failed", task=kwargs.get("task"), error="rate_limited", attempts=attempt)
                return None
            delay = _RETRY_BASE_SECONDS * attempt + random.uniform(0, 1)
            logger.warning(
                "kira_rate_limited_retrying",
                attempt=attempt,
                delay_seconds=round(delay, 1),
                task=kwargs.get("task"),
            )
            await asyncio.sleep(delay)
        except Exception as exc:
            logger.error("kira_call_failed", task=kwargs.get("task"), error=str(exc))
            return None
    return None


async def classify_hashtag_relevance(root_hashtag: str, candidate_hashtag: str) -> bool | None:
    """True when candidate looks specific to root, False when generic, None
    when Kira is off/unconfigured/failed - callers keep the candidate."""
    result = await _complete_with_retry(
        task="hashtag_bfs",
        user_prompt=f'ROOT: "{root_hashtag}"\nCANDIDATE: "{candidate_hashtag}"',
        system_prompt=_resolve_prompt("hashtag_bfs"),
        max_tokens=800,
        platform="tiktok",
        extra={"root": root_hashtag, "candidate": candidate_hashtag},
    )
    if result is None or not result.ok:
        return None
    verdict = result.content.strip().lower()
    if verdict not in ("relevant", "generic"):
        logger.warning(
            "kira_hashtag_relevance_failed",
            root=root_hashtag,
            candidate=candidate_hashtag,
            error=f"unexpected verdict: {verdict!r}",
        )
        return None
    return verdict == "relevant"


def suggest_element_index(goal: str, candidates: list[str]) -> int | None:
    if not kira_is_enabled():
        return None
    listing = "\n".join(f"{i}: {c}" for i, c in enumerate(candidates))
    try:
        result = _complete_sync(
            task="selector",
            user_prompt=f"GOAL: {goal}\n\nCANDIDATES:\n{listing}",
            system_prompt=_resolve_prompt("selector"),
            max_tokens=600,
            extra={"goal": goal, "candidate_count": len(candidates)},
        )
    except Exception:
        return None
    if not result.ok:
        return None
    text = result.content.strip().lower()
    if text == "none":
        return None
    try:
        index = int(text)
    except ValueError:
        logger.warning("kira_selector_suggestion_failed", goal=goal, error=f"unexpected index: {text!r}")
        return None
    if not (0 <= index < len(candidates)):
        logger.warning("kira_selector_suggestion_failed", goal=goal, error=f"index {index} out of range")
        return None
    return index


def diagnose_account_failure(reason: str) -> str | None:
    if not kira_is_enabled():
        return None
    try:
        result = _complete_sync(
            task="diagnosis",
            user_prompt=reason,
            system_prompt=_resolve_prompt("diagnosis"),
            max_tokens=600,
            extra={"reason": _preview(reason, 500)},
        )
    except Exception:
        return None
    text = (result.content or "").strip()
    return text or None


# Kept so existing `from ...kira import KIRA_ENABLED` still resolves.
# Runtime switch is kira_is_enabled() / ai_settings.enabled.
KIRA_ENABLED = False
