"""
Facebook comments spider that never opens a browser: fetches comments for a
post via curl_cffi, using the query cached by `bootstrap_comments()`
(social_crawler.spiders.facebook.auth.bootstrap).

Pass -a dedupe=false to disable cross-run dedupe (e.g. to re-fetch comments
already seen in a previous run) - it's on by default whenever Redis is
reachable, and silently falls back to in-run-only dedupe otherwise.

Run:
    scrapy crawl facebook_comments -a post_id="122197539992842674" -a max_pages=3
"""

from __future__ import annotations

import asyncio

import scrapy

from social_crawler.constants.facebook import SEEN_COMMENTS_KEY
from social_crawler.logger import get_logger
from social_crawler.services.kafka import RAW_COMMENTS_TOPIC, KafkaPublisher
from social_crawler.services.redis import RedisCache, enable_dedupe_cache
from social_crawler.spiders.facebook.auth.graphql_client import (
    CheckpointRequiredError,
    FacebookGraphQLClient,
    RateLimitedError,
    SessionExpiredError,
)
from social_crawler.spiders.facebook.features.comments.extract import (
    extract_comments,
    extract_replies,
    find_comments_page_info,
    find_replies_page_info,
)
from social_crawler.spiders.facebook.items import FacebookCommentItem

logger = get_logger(__name__)

# Ceiling on how many *pages* of replies one top-level comment's own thread
# will page through (each page is its own GraphQL request) - separate from
# the top-level comments loop's own max_pages, not reused from it: a post
# can have many top-level comments, and every one of them with
# replies_count>0 gets its own _fetch_replies call, so this bounds the
# per-comment cost rather than the per-post one. Raised from an original 3
# (2026-09-15) to a much higher safety ceiling (2026-09-16, explicit "get
# every comment, no capping" request) - same role as Facebook search's own
# max_pages=100 "safety ceiling, not a target" (see that spider's own
# comment): find_replies_page_info/get_replies_next_page already stop
# naturally the moment has_next_page is false, so this only matters for a
# genuinely enormous reply thread, and exists purely so a page_info bug
# can't spin forever rather than to cap real completeness.
MAX_REPLY_PAGES = 200


class FacebookCommentsSpider(scrapy.Spider):
    name = "facebook_comments"

    custom_settings = {"ROBOTSTXT_OBEY": False}

    def __init__(
        self,
        post_id: str | None = None,
        count: int = -1,
        max_pages: int = 1,
        dedupe: str = "true",
        account: str | None = None,
        include_replies: str = "false",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.post_id = post_id
        # -1 = Comet's densest CommentsListComponentsPaginationQuery page
        # (browser default). Positive values still work but Facebook often
        # ignores them and returns ~10 anyway.
        self.count = int(count)
        self.max_pages = int(max_pages)
        self.dedupe_enabled = str(dedupe).lower() not in ("false", "0", "no")
        # Off by default: each top-level comment with replies_count>0 used
        # to fire an extra GraphQL round trip, and the replies query path
        # currently returns empty edges for most accounts - burning
        # throttle budget without yielding replies. Pass include_replies=true
        # once that capture is fixed.
        self.include_replies = str(include_replies).lower() not in ("false", "0", "no")
        # Pinned by crawl_request_consumer.py to whichever account it just
        # verified (or freshly bootstrapped) has a usable comments-query
        # cache - see its own comment for why this must not fall back to
        # FacebookGraphQLClient's default of reading ACTIVE_ACCOUNT_REDIS_KEY
        # itself (that key can change between the consumer's check and this
        # subprocess actually starting). None only for a manual
        # `scrapy crawl facebook_comments` run from the CLI, where reading
        # ACTIVE_ACCOUNT_REDIS_KEY directly is the correct, only option.
        self.account = account
        self._cache: RedisCache | None = None
        self._kafka = KafkaPublisher()

    async def _fetch_replies(self, client: FacebookGraphQLClient, comment_id: str, legacy_comment_id: str):
        """Fetch and yield the replies to one top-level comment, paging up
        to MAX_REPLY_PAGES deep via get_replies_next_page/
        find_replies_page_info - mirrors the top-level comments loop in
        start() below, just re-rooted at a comment instead of a post."""
        cursor: str | None = None
        page = 1
        total = 0
        while True:
            try:
                if cursor is None:
                    response = await asyncio.to_thread(client.get_replies, legacy_comment_id)
                else:
                    response = await asyncio.to_thread(
                        client.get_replies_next_page, legacy_comment_id, cursor, self.count
                    )
            except SessionExpiredError as exc:
                if page == 1:
                    logger.error(
                        "session_expired",
                        error=str(exc),
                        hint='python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "<a post url>" --type replies',
                    )
                else:
                    # get_replies_next_page raises this same error when no
                    # *paginated* replies query has been captured yet (see
                    # its own docstring) - distinct from page 1's own
                    # session actually being dead, since get_replies (page
                    # 1) just succeeded. Stop here rather than abort the
                    # whole comment (its first page already yielded);
                    # bootstrap.py --type replies against a comment with
                    # more replies than fit on one page is what's missing.
                    logger.warning("replies_pagination_not_bootstrapped", parent_comment_id=comment_id, error=str(exc))
                return
            except CheckpointRequiredError as exc:
                # _run() already disabled the account and sent the Telegram
                # alert (see comet_graphql_client.py).
                logger.error("checkpoint_required", error=str(exc))
                return
            except RateLimitedError as exc:
                logger.error("rate_limited", telegram=True, error=str(exc))
                return

            replies = extract_replies(response)
            for reply in replies:
                reply_id = reply.get("comment_id")
                if not reply_id:
                    logger.warning("reply_missing_id", parent_comment_id=comment_id)
                    continue
                if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, reply_id) == 0:
                    continue
                total += 1
                await self._kafka.publish(
                    topic=RAW_COMMENTS_TOPIC,
                    key=f"facebook:{reply_id}",
                    value={
                        "platform": "facebook",
                        "post_id": self.post_id,
                        "parent_comment_id": comment_id,
                        **reply,
                    },
                )
                yield FacebookCommentItem(post_id=self.post_id, parent_comment_id=comment_id, **reply)

            logger.info("replies_page_crawled", parent_comment_id=comment_id, page=page, new_replies=len(replies))

            # find_replies_page_info's own docstring flags its response
            # path as an unconfirmed best-guess (never seen a real
            # multi-page replies response) - failing closed here (treat a
            # missing/empty page_info as "no more pages" rather than
            # erroring) means a wrong guess costs nothing worse than the
            # original single-page behavior, not a crash.
            page_info = find_replies_page_info(response)
            if page >= MAX_REPLY_PAGES or not page_info or not page_info.get("has_next_page"):
                break
            cursor = page_info.get("end_cursor")
            if not cursor:
                break
            page += 1

        logger.info("replies_fetched", parent_comment_id=comment_id, pages=page, count=total)

    async def start(self):
        if not self.post_id:
            logger.error("missing_post_id", hint='scrapy crawl facebook_comments -a post_id="<a post id>"')
            return

        await self._kafka.start()

        if self.dedupe_enabled:
            self._cache = enable_dedupe_cache(logger)

        try:
            # Reuse this spider's own RedisCache/connection instead of
            # letting the client open a second, independent one internally.
            client = FacebookGraphQLClient(redis_cache=self._cache, account=self.account)
        except SessionExpiredError as exc:
            logger.error("session_expired", error=str(exc))
            return
        except RateLimitedError as exc:
            logger.error("rate_limited", telegram=True, error=str(exc))
            return

        cursor: str | None = None
        page = 1
        total_count = 0

        try:
            while True:
                try:
                    if cursor is None:
                        response = await asyncio.to_thread(client.get_comments, self.post_id)
                    else:
                        response = await asyncio.to_thread(
                            client.get_comments_next_page, self.post_id, cursor, self.count
                        )
                except SessionExpiredError as exc:
                    logger.error(
                        "session_expired",
                        error=str(exc),
                        hint='python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "<a post url>"',
                    )
                    return
                except CheckpointRequiredError as exc:
                    # _run() already disabled the account and sent the
                    # Telegram alert (see comet_graphql_client.py).
                    logger.error("checkpoint_required", error=str(exc))
                    return
                except RateLimitedError as exc:
                    logger.error("rate_limited", telegram=True, error=str(exc))
                    return

                comments = extract_comments(response)
                new_count = 0
                for comment in comments:
                    comment_id = comment.get("comment_id")
                    if not comment_id:
                        logger.warning("comment_missing_id", post_id=self.post_id)
                        continue
                    # sadd()'s return value already answers "was this new" in one
                    # atomic round trip - no separate sismember check needed (and
                    # no race between a check and a later add).
                    if self._cache and self._cache.sadd(SEEN_COMMENTS_KEY, comment_id) == 0:
                        continue
                    new_count += 1
                    total_count += 1
                    await self._kafka.publish(
                        topic=RAW_COMMENTS_TOPIC,
                        key=f"facebook:{comment_id}",
                        # extract_comments' dict has no post_id of its own
                        # (it's per-comment, not per-response) - without
                        # this, every raw_comments message is missing the
                        # one field a consumer needs to know which post a
                        # comment belongs to (confirmed happening for real:
                        # cinemark-api's ingest_consumer logging
                        # comment_unknown_post post_id=None for every
                        # comment). FacebookCommentItem below gets it right
                        # already (post_id=self.post_id passed explicitly);
                        # this just brings the Kafka payload to parity.
                        value={"platform": "facebook", "post_id": self.post_id, **comment},
                    )
                    yield FacebookCommentItem(post_id=self.post_id, **comment)

                    if (
                        self.include_replies
                        and comment.get("replies_count")
                        and comment.get("legacy_comment_id")
                    ):
                        async for reply_item in self._fetch_replies(
                            client, comment_id, comment["legacy_comment_id"]
                        ):
                            total_count += 1
                            yield reply_item

                logger.info("page_crawled", page=page, new_comments=new_count, fetched=len(comments))

                page_info = find_comments_page_info(response)
                if page >= self.max_pages or not page_info or not page_info.get("has_next_page"):
                    break
                cursor = page_info.get("end_cursor")
                if not cursor:
                    break
                page += 1
        finally:
            await self._kafka.stop()

        logger.info("crawl_finished", telegram=True, post_id=self.post_id, comments=total_count, pages=page)
