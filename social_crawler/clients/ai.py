"""Client AI của spider-hub - mọi lời gọi đi qua Bee (Beeknoee, tương thích OpenAI), anh em với app/ai/client.py của
cinemark-api. Kira đã bỏ hẳn từ 2026-10-21 (chậm, hay 429/504).

Mọi thứ lấy từ cùng các dòng Postgres mà tab AI trong Settings của dashboard cinemark-api sửa - không dùng biến env:
  - ai_providers key="bee": base_url, api_key, model (model dùng cho các task chat: diagnosis, selector)
  - ai_settings: công tắc enabled + prompt hệ thống ghi đè theo từng task
Thiếu DB/thông tin đăng nhập/model thì thành no-op.

Phân loại hashtag (BFS TikTok) không dùng chat mà dùng model quyết định qua POST /v1/decisions của Bee
(HASHTAG_DECISION_MODEL): trả về lựa chọn kèm độ tự tin, ~0,8 giây/câu. Đo 2026-10-21 trên 30 cặp hashtag thật:
gpt-6-luna-decisions 29/30 (jev-1.14 26/30 - hụt hashtag tên diễn viên không dấu); cộng so khớp tên phim/diễn viên bỏ dấu
và "độ tự tin thấp thì giữ" thì 30/30, không bỏ nhầm hashtag liên quan nào.
"""

from __future__ import annotations

import asyncio
import random
import re
import time
import unicodedata
from dataclasses import dataclass, field
from typing import Any

import openai
import requests
from openai import OpenAI

from social_crawler.logger import get_logger

logger = get_logger(__name__)

_PROVIDER_KEY = "bee"
HASHTAG_DECISION_MODEL = "openai/gpt-6-luna-decisions"
# Dưới ngưỡng này coi như "không chắc" -> giữ ứng viên (fail-open), giống khi lỗi. Các lần hụt khi đo đều < 0,5.
_HASHTAG_MIN_CONFIDENCE = 0.5
_MAX_RATE_LIMIT_RETRIES = 3
_RETRY_BASE_SECONDS = 2.0
# Mặc định của SDK (timeout 600s + tự thử lại 2 lần với 5xx) làm một lần gateway trả 504 treo mỗi ứng viên hashtag vài
# phút - BFS gọi tuần tự nên cả lượt crawl TikTok kẹt theo (2026-10-21).
_REQUEST_TIMEOUT_SECONDS = 45.0
# Sau một lỗi không phải rate limit (504, timeout, mất kết nối), bỏ qua AI trong khoảng này: chỗ gọi đều fail-open
# (giữ ứng viên) nên chỉ mất phần lọc, không mất dữ liệu.
_FAILURE_COOLDOWN_SECONDS = 300.0
_cooldown_until = 0.0
_PROMPT_LOG_CHARS = 2000
_CONTENT_LOG_CHARS = 4000

_HASHTAG_QUESTION = {
    "type": "choice",
    "instructions": "Hashtag ứng viên có ĐẶC THÙ cho bộ phim của hashtag gốc không? relevant khi nó nhắc tên phim (kể cả "
    "viết tắt, thêm năm/số phần), diễn viên, đạo diễn, nhân vật hoặc nội dung riêng của phim. generic khi nó là hashtag "
    "chung (thể loại, nền tảng, xu hướng, rạp, từ thông dụng) dùng cho rất nhiều nội dung khác.",
    "criteria": {"relevant": "Đặc thù cho bộ phim này", "generic": "Chung chung, không riêng bộ phim này"},
}

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
    "diagnosis": _DIAGNOSIS_SYSTEM_PROMPT,
    "selector": _SELECTOR_SYSTEM_PROMPT,
}

_client: OpenAI | None = None
# (base_url, api_key) mà client đã cache được dựng cùng - xoay key hoặc đổi base_url từ
# dashboard thì client được dựng lại ở lời gọi kế tiếp.
_client_identity: tuple[str, str] | None = None
_ai_cfg_cache: tuple[float, dict[str, Any]] | None = None
# Mỗi lần đọc mở 2 kết nối Supabase mới (vài giây) - TTL 5s cũ làm mỗi ứng viên hashtag BFS chờ ~14s thay vì ~1s
# (2026-10-21). Đổi cấu hình trên dashboard thì spider-hub nhận trong vòng một phút.
_AI_CFG_TTL_SECONDS = 60.0


def _preview(text: str, limit: int) -> str:
    value = text or ""
    if len(value) <= limit:
        return value
    return f"{value[:limit]}...<{len(value) - limit} more chars>"


@dataclass
class AiUsage:
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
class AiResponse:
    """Cùng các trường với app.ai.client.AIResponse của cinemark-api."""

    ok: bool
    task: str
    model: str
    content: str = ""
    finish_reason: str | None = None
    usage: AiUsage | None = None
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
    cfg: dict[str, Any] = {"enabled": False, "model": "", "prompts": {}, "base_url": "", "api_key": ""}
    try:
        from social_crawler.db.config import get_ai_provider, get_ai_settings

        settings = get_ai_settings()
        provider = get_ai_provider(_PROVIDER_KEY) or {}
        cfg = {
            "enabled": settings.get("enabled", False),
            "prompts": settings.get("prompts") or {},
            "model": (provider.get("model") or "").strip(),
            "base_url": (provider.get("base_url") or "").strip(),
            "api_key": (provider.get("api_key") or "").strip(),
        }
    except Exception as exc:  # noqa: BLE001 - cấu hình AI không bao giờ được làm hỏng một lượt crawl
        logger.warning("ai_settings_load_failed", error=exc)
        if _ai_cfg_cache is not None:
            return _ai_cfg_cache[1]
    _ai_cfg_cache = (now, cfg)
    return cfg


def ai_is_enabled() -> bool:
    return bool(_load_ai_runtime().get("enabled"))


def _resolve_prompt(task: str) -> str:
    stored = _load_ai_runtime().get("prompts") or {}
    custom = (stored.get(task) or "").strip()
    if custom:
        return custom
    return _CODE_DEFAULT_PROMPTS.get(task, "")


def _get_client() -> OpenAI | None:
    global _client, _client_identity
    cfg = _load_ai_runtime()
    base_url, api_key = cfg.get("base_url") or "", cfg.get("api_key") or ""
    if not base_url or not api_key:
        _client, _client_identity = None, None
        return None
    if _client is None or _client_identity != (base_url, api_key):
        _client = OpenAI(base_url=base_url, api_key=api_key, timeout=_REQUEST_TIMEOUT_SECONDS, max_retries=0)
        _client_identity = (base_url, api_key)
    return _client


def _complete_sync(
    *,
    task: str,
    user_prompt: str,
    system_prompt: str,
    max_tokens: int,
    platform: str | None = None,
    extra: dict[str, Any] | None = None,
) -> AiResponse:
    client = _get_client()
    model = _load_ai_runtime().get("model") or ""
    if client is None or not model:
        result = AiResponse(
            ok=False,
            task=task,
            model=model,
            error="ai_not_configured" if client is None else "ai_model_not_configured",
            platform=platform,
            extra=extra or {},
        )
        logger.warning("ai_call_failed", **result.as_log_fields())
        return result

    logger.info(
        "ai_call_started",
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
        result = AiResponse(
            ok=False,
            task=task,
            model=model,
            latency_ms=latency_ms,
            error=str(exc),
            platform=platform,
            extra=extra or {},
        )
        logger.error("ai_call_failed", **result.as_log_fields())
        raise

    latency_ms = int((time.perf_counter() - started) * 1000)
    choice = completion.choices[0] if completion.choices else None
    content = (choice.message.content if choice and choice.message else None) or ""
    finish_reason = getattr(choice, "finish_reason", None) if choice else None
    raw_usage = getattr(completion, "usage", None)
    usage = None
    if raw_usage is not None:
        usage = AiUsage(
            prompt_tokens=getattr(raw_usage, "prompt_tokens", None),
            completion_tokens=getattr(raw_usage, "completion_tokens", None),
            total_tokens=getattr(raw_usage, "total_tokens", None),
        )
    result = AiResponse(
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
    logger.info("ai_call_finished", **result.as_log_fields())
    return result


class _RateLimited(Exception):
    pass


def _decide_sync(state: str, questions: dict[str, dict[str, Any]], *, task: str, extra: dict[str, Any]) -> dict | None:
    """Một lần POST /v1/decisions (model quyết định của Bee). Trả về `answers`, None khi chưa cấu hình."""
    cfg = _load_ai_runtime()
    base_url, api_key = cfg.get("base_url") or "", cfg.get("api_key") or ""
    if not base_url or not api_key:
        logger.warning("ai_call_failed", task=task, error="ai_not_configured", ok=False)
        return None
    started = time.perf_counter()
    resp = requests.post(
        f"{base_url.rstrip('/')}/decisions",
        json={"model": HASHTAG_DECISION_MODEL, "state": state, "questions": questions},
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=_REQUEST_TIMEOUT_SECONDS,
    )
    latency_ms = int((time.perf_counter() - started) * 1000)
    if resp.status_code == 429:
        raise _RateLimited()
    if resp.status_code >= 400:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
    answers = (resp.json() or {}).get("answers") or {}
    logger.info(
        "ai_call_finished",
        task=task,
        model=HASHTAG_DECISION_MODEL,
        latency_ms=latency_ms,
        answers={key: (a.get("choice"), a.get("confidence")) for key, a in answers.items()},
        **extra,
    )
    return answers


async def _with_retry(task: str, fn) -> Any:
    """Chạy `fn` (đồng bộ) trong thread: thử lại khi bị rate limit, lỗi khác thì nghỉ _FAILURE_COOLDOWN_SECONDS.
    None khi AI tắt / đang nghỉ / lỗi - mọi chỗ gọi đều fail open."""
    global _cooldown_until
    if not ai_is_enabled() or time.monotonic() < _cooldown_until:
        return None
    for attempt in range(1, _MAX_RATE_LIMIT_RETRIES + 1):
        try:
            return await asyncio.to_thread(fn)
        except (openai.RateLimitError, _RateLimited):
            if attempt == _MAX_RATE_LIMIT_RETRIES:
                logger.error("ai_call_failed", task=task, error="rate_limited", attempts=attempt)
                return None
            delay = _RETRY_BASE_SECONDS * attempt + random.uniform(0, 1)
            logger.warning("ai_rate_limited_retrying", attempt=attempt, delay_seconds=round(delay, 1), task=task)
            await asyncio.sleep(delay)
        except Exception as exc:
            _cooldown_until = time.monotonic() + _FAILURE_COOLDOWN_SECONDS
            logger.error(
                "ai_call_failed", task=task, error=str(exc)[:300], cooldown_seconds=int(_FAILURE_COOLDOWN_SECONDS)
            )
            return None
    return None


def _fold(text: str) -> str:
    """Bỏ dấu, chữ thường, chỉ giữ a-z0-9 - "Steven Nguyễn" -> "stevennguyen", khớp được hashtag viết liền không dấu."""
    text = unicodedata.normalize("NFD", (text or "").lower().replace("đ", "d"))
    return re.sub(r"[^a-z0-9]", "", text.encode("ascii", "ignore").decode())


def _film_names(movie_context: str | None) -> list[str]:
    """Tên phim, đạo diễn, diễn viên từ khối movie_context của cinemark-api (app/ai/movie_context.py)."""
    names: list[str] = []
    for line in (movie_context or "").splitlines():
        label, _, value = line.partition(":")
        if label.strip().lower() in ("target film", "director", "cast"):
            value = re.sub(r"\(\+\d+ more\)", "", value)
            names += [part.strip() for part in value.split(",") if part.strip()]
    return names


async def classify_hashtag_relevance(
    root_hashtag: str, candidate_hashtag: str, movie_context: str | None = None
) -> bool | None:
    """True khi ứng viên đặc thù cho gốc, False khi chung chung, None khi AI tắt/chưa cấu hình/lỗi/không chắc - chỗ gọi
    giữ lại ứng viên. Ứng viên chứa tên phim / đạo diễn / diễn viên (bỏ dấu, viết liền) là relevant luôn, không cần hỏi AI:
    model quyết định hay hụt đúng loại này."""
    candidate = _fold(candidate_hashtag)
    if any(len(name) >= 5 and name in candidate for name in map(_fold, _film_names(movie_context))):
        logger.info("bfs_hashtag_name_match", root=root_hashtag, candidate=candidate_hashtag)
        return True
    state = f"Hashtag gốc: #{root_hashtag}"
    if movie_context:
        state += f"\nPhim:\n{movie_context}"
    state += f"\nHashtag ứng viên: #{candidate_hashtag}"
    extra = {"root": root_hashtag, "candidate": candidate_hashtag, "platform": "tiktok"}
    answers = await _with_retry(
        "hashtag_bfs", lambda: _decide_sync(state, {"verdict": _HASHTAG_QUESTION}, task="hashtag_bfs", extra=extra)
    )
    verdict = (answers or {}).get("verdict") or {}
    choice, confidence = verdict.get("choice"), float(verdict.get("confidence") or 0)
    if choice not in ("relevant", "generic") or confidence < _HASHTAG_MIN_CONFIDENCE:
        return None
    return choice == "relevant"


def suggest_element_index(goal: str, candidates: list[str]) -> int | None:
    if not ai_is_enabled():
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
        logger.warning("ai_selector_suggestion_failed", goal=goal, error=f"unexpected index: {text!r}")
        return None
    if not (0 <= index < len(candidates)):
        logger.warning("ai_selector_suggestion_failed", goal=goal, error=f"index {index} out of range")
        return None
    return index


def diagnose_account_failure(reason: str) -> str | None:
    if not ai_is_enabled():
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
