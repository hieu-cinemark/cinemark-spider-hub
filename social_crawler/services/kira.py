"""Thin Kira (OpenAI-compatible LLM) client - sibling to cinemark-api's own
app/kira/base.py, kept as an independent implementation rather than a
shared import since spider-hub and cinemark-api are separate deployables
(same convention services/error_alerts.py's docstring calls out for its
own cinemark-api counterpart).

The only caller today is hashtag_search/search.py's BFS candidate filter
(classify_hashtag_relevance below) - unlike cinemark-api's KiraAI, which
raises at import if unconfigured, the client here degrades to None so a
missing/misconfigured KIRA_API_KEY only turns off that one optional filter
instead of breaking every TikTok hashtag crawl."""

from __future__ import annotations

import asyncio
import os
import random

import openai
from openai import OpenAI

from social_crawler import env  # noqa: F401 - import for its load_dotenv() side effect
from social_crawler.logger import get_logger

logger = get_logger(__name__)

_MODEL = "kira-3.5-flash"

# Caps concurrent Kira calls process-wide - _queue_bfs_hashtags already
# calls these one at a time in a loop (not concurrently), but a Facebook/
# Threads/TikTok crawl each running their own spider process at once would
# otherwise still pile up simultaneous requests against the same provider
# account with no coordination between them.
_KIRA_CONCURRENCY = asyncio.Semaphore(2)
_MAX_RATE_LIMIT_RETRIES = 3
_RETRY_BASE_SECONDS = 2.0

_SYSTEM_PROMPT = """
You are a hashtag relevance classifier for a TikTok content-discovery pipeline.

You are given a ROOT hashtag (what a crawler was already searching for,
usually a movie title or a closely related term) and a CANDIDATE hashtag
that co-occurred with it on multiple videos.

Decide whether the CANDIDATE is topically specific to the ROOT (the same
movie, franchise, cast, or a clearly related term) or whether it is a
generic/unrelated tag that would just as likely co-occur with completely
unrelated content (e.g. "#fyp", "#xuhuong", "#reviewphim", "#hot", a
platform meme tag, or an unrelated movie/brand name).

Respond with ONLY one word, lowercase, no punctuation: "relevant" or "generic".
"""

_client: OpenAI | None = None
_client_checked = False


def _get_client() -> OpenAI | None:
    """Lazy singleton so importing this module never requires KIRA_API_KEY
    to be set - most spider-hub setups (CI, ad-hoc `scrapy crawl` runs)
    have no need for it at all."""
    global _client, _client_checked
    if _client_checked:
        return _client
    _client_checked = True
    api_key = os.getenv("KIRA_API_KEY")
    base_url = os.getenv("KIRA_BASE_URL")
    if api_key and base_url:
        _client = OpenAI(base_url=base_url, api_key=api_key)
    return _client


async def _create_completion(client: OpenAI, *, messages: list[dict[str, str]]):
    """Runs one Kira chat completion, capped to _KIRA_CONCURRENCY concurrent
    calls and retried with backoff on a 429 (RateLimitError) specifically -
    anything else (bad JSON, auth, network) fails immediately since a retry
    won't help. temperature/max_tokens are fixed here rather than passed in
    since every caller today wants the same deterministic, low-token
    classification shape."""
    async with _KIRA_CONCURRENCY:
        for attempt in range(1, _MAX_RATE_LIMIT_RETRIES + 1):
            try:
                return await asyncio.to_thread(
                    client.chat.completions.create,
                    model=_MODEL,
                    messages=messages,
                    temperature=0.0,
                    # kira-3.5-flash is a reasoning model - it spends a few
                    # hundred tokens of reasoning_content before ever
                    # writing the one-word answer to content, so a tight
                    # max_tokens truncates mid-thought (finish_reason=
                    # "length", content="") rather than saving cost.
                    max_tokens=800,
                )
            except openai.RateLimitError:
                if attempt == _MAX_RATE_LIMIT_RETRIES:
                    raise
                delay = _RETRY_BASE_SECONDS * attempt + random.uniform(0, 1)
                logger.warning("kira_rate_limited_retrying", attempt=attempt, delay_seconds=round(delay, 1))
                await asyncio.sleep(delay)


async def classify_hashtag_relevance(root_hashtag: str, candidate_hashtag: str) -> bool | None:
    """True when `candidate_hashtag` looks topically specific to
    `root_hashtag`, False when it looks generic, None when Kira isn't
    configured or the call/parse failed - callers should treat None as
    "don't know" and queue the candidate anyway rather than let a missing
    key or an LLM hiccup silently shrink BFS coverage."""
    client = _get_client()
    if client is None:
        return None
    try:
        completion = await _create_completion(
            client,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": f'ROOT: "{root_hashtag}"\nCANDIDATE: "{candidate_hashtag}"'},
            ],
        )
        verdict = (completion.choices[0].message.content or "").strip().lower()
        if verdict not in ("relevant", "generic"):
            raise ValueError(f"unexpected verdict: {verdict!r}")
        return verdict == "relevant"
    except Exception as exc:
        logger.warning(
            "kira_hashtag_relevance_failed", root=root_hashtag, candidate=candidate_hashtag, error=str(exc)
        )
        return None
