"""Opens a visible browser logged into the currently-rotated Threads
account's existing session, so a human can look at / resolve an in-browser
checkpoint challenge that Meta raised mid-session (see graphql_client.py's
`checkpoint_required` response body) - separate from bootstrap.py's full
login+capture flow, which assumes the account already replies to search
requests cleanly and would otherwise try to re-run search capture against
an account that can't search yet.

Run:
    python -m social_crawler.spiders.threads.auth.open_browser

Reuses whatever storage_state is already cached for the rotated account
(the same one bootstrap.py's normal runs use) - if none exists yet, this
falls back to bootstrap.py's own login flow instead. Leaves the browser
open until you press Enter here, then saves the resulting cookies back to
Redis so the next `bootstrap.py --query "..."` run picks up the
now-resolved session instead of the checkpointed one.
"""

from __future__ import annotations

from patchright.sync_api import sync_playwright

from social_crawler.constants.threads import STATE_REDIS_KEY_TMPL
from social_crawler.logger import get_logger
from social_crawler.services.redis import RedisCache
from social_crawler.spiders.threads.auth.bootstrap import _get_authenticated_context

logger = get_logger(__name__)


def main() -> None:
    redis_cache = RedisCache()
    with sync_playwright() as pw:
        browser, context, page, account_key = _get_authenticated_context(pw, redis_cache, headless=False)
        try:
            page.goto("https://www.threads.com/")
            logger.info(
                "browser_open_for_manual_inspection",
                account=account_key,
                hint="resolve any checkpoint/verification prompt here, then press Enter in this terminal",
            )
            input()
        finally:
            redis_cache.set(STATE_REDIS_KEY_TMPL.format(account=account_key), context.storage_state())
            browser.close()
            logger.info("saved_session_after_manual_inspection", account=account_key)


if __name__ == "__main__":
    main()
