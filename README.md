# spider-hub

A multi-platform social-media data-collection service built on Scrapy, used
to feed the Cinemark social-listening pipeline. Each platform lives in its
own package under `social_crawler/spiders/<platform>/` with whatever
login/session/anti-bot handling that platform needs; **Facebook is the first
integration** (see below) — more platforms (e.g. TikTok, Threads) are
expected to be added the same way over time.

## How this fits together

In production, crawls aren't run ad-hoc - they're driven end-to-end by
Kafka, triggered by the sibling `cinemark-api` project:

```
cinemark-api                          spider-hub                        cinemark-scraper (D1)
  POST /facebook/run                                                     
  (manual button, or                                                     
  scripts/trigger_scheduled_crawl.sh  
  every 6h)                           
      │                                                                  
      ▼ reads enabled keywords from D1 (cinemark-scraper's own tables)   
      │                                                                  
      ▼ Kafka "crawl_requests"                                           
      ─────────────────────────────►  crawl_request_consumer.py          
                                       (this repo, systemd service)       
                                       runs `scrapy crawl facebook_search`
                                           │                              
                                           ▼ Kafka "raw_posts"            
                                       ─────────────────────────────────► cinemark-api's
                                                                           ingest_consumer
                                                                           writes into D1's
                                                                           `posts` table
```

`crawl_request_consumer.py` is the real entry point - see "Kafka-driven
crawls" below. Everything under "Usage" (`scrapy crawl ...` by hand) is for
local development and debugging a single spider in isolation, not how
crawls happen day to day.

## Facebook integration

Facebook's web GraphQL API isn't public, and a plain HTTP client gets
flagged instantly by TLS fingerprinting. This integration works around both
problems in two stages:

1. **Bootstrap (browser, one-time/periodic)** — [`patchright`](https://github.com/Kaliiiiiiiiii-Vinyzu/patchright)
   (a CDP-leak-patched Playwright fork) drives a real Chromium session to log
   in (or reuse a saved session / imported cookies), performs a search or
   opens a post, and captures the real GraphQL request Facebook's own
   frontend fires. The `doc_id`, tokens, cookies and headers from that
   request are cached in Redis.
2. **Crawl (HTTP only, every run)** — the Scrapy spiders never open a
   browser. They replay the cached request via
   [`curl_cffi`](https://github.com/lexiforest/curl_cffi) (which impersonates
   a real Chrome TLS/JA3 fingerprint) directly against Facebook's GraphQL
   endpoint, swapping in a new search query / cursor / date filter each
   time. This is why the spiders bypass Scrapy's own downloader entirely —
   `ROBOTSTXT_OBEY` and the downloader middlewares don't apply here.

Everything session-related (login cookies, the captured token cache, account
rotation state, cross-run dedupe sets) lives in Redis, not on disk, so
multiple accounts and multiple machines can share the same state.

This browser-bootstrap-then-HTTP-replay approach is specific to what
Facebook's web app requires - it isn't a pattern every future platform has
to follow. A platform with a stable public API, or one served via a paid
reseller (the sibling `cinemark-scraper` project takes exactly this simpler
approach for its own TikTok/Threads/Facebook integrations), wouldn't need
browser automation at all. See "Adding a new platform" below.

## Facebook features

- **`facebook_search` spider** — keyword search, with cursor pagination,
  Vietnamese/English locale support, an optional `start_date`/`end_date`
  window filter, and a "sweep" mode that walks many small date windows to
  get past Facebook's per-query result cap. Posts and other entities
  (pages, groups, hashtags...) are extracted and deduped (in-run and,
  optionally, across runs via Redis).
- **`facebook_comments` spider** — paginated comments for a given post,
  sorted "Newest" first.
- **Account rotation** — cycles through multiple configured Facebook
  accounts (`FACEBOOK_ACCOUNTS`) across bootstrap runs, atomically, so
  repeated logins spread across accounts instead of hammering one.
- **Resilience** — retries with backoff on rate limiting (429) and server
  errors, distinguishes a genuinely dead session from a rate-limited one,
  retries transient Redis write failures, and never leaks the login
  browser process on failure.
- **Telegram alerting** — every warning/error, plus a few completion
  milestones, is optionally pushed to a Telegram chat so an unattended cron
  run doesn't fail silently.
- **Structured logging** — via `structlog`, platform-tagged console output.

## Requirements

- Python 3.11+
- A running Redis instance
- Chromium (installed automatically for `patchright` — see setup below)

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
patchright install chromium
```

Create a `.env` file in the project root (or export these as real env vars):

```bash
# Kafka - only needed to run crawl_request_consumer.py (the production
# entry point, see "Kafka-driven crawls" below). Not needed for ad-hoc
# `scrapy crawl ...` runs.
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Redis - session cache, token cache, account rotation, dedupe sets
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# Optional: log in automatically instead of pausing for manual browser login.
# JSON array, every key required (use "" for anything an account doesn't have).
# See social_crawler/spiders/facebook/auth/accounts.py for field details.
FACEBOOK_ACCOUNTS=[{"id":"you@example.com","password":"...","2fa":"","cookie":"","token":"","email":""}]

# Optional: proxy for the ongoing curl_cffi replay traffic (recommended -
# spreads load across IPs). Scheme must NOT be included in the value.
FACEBOOK_PROXY_URL=<proxy_host>:<proxy_port>
FACEBOOK_PROXY_USERNAME=
FACEBOOK_PROXY_PASSWORD=
# Whether the one-time login browser should also use the proxy above (off by
# default - a proxy IP that doesn't match the account's usual geography is
# what triggers a captcha on a fresh login).
FACEBOOK_LOGIN_USE_PROXY=false

# Optional: push warnings/errors/milestones to a Telegram chat
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

## Usage

**1. Bootstrap once** to capture a search token cache (opens a visible
browser the first time; reuses the saved session headlessly after that):

```bash
python -m social_crawler.spiders.facebook.auth.bootstrap --query "test"
```

Bootstrap the comments feature the same way, against a real post URL:

```bash
python -m social_crawler.spiders.facebook.auth.bootstrap --post-url "https://www.facebook.com/.../posts/..."
```

Already have cookies from an existing logged-in session? Skip the browser
login entirely:

```bash
python -m social_crawler.spiders.facebook.auth.bootstrap --cookies-file my_cookies.json
```

**2. Crawl:**

```bash
scrapy crawl facebook_search -a query="keyword" -a count=10 -a max_pages=5

# Date-filtered:
scrapy crawl facebook_search -a query="keyword" -a start_date=2026-08-01 -a end_date=2026-08-15

# Sweep past Facebook's per-query result cap:
scrapy crawl facebook_search -a query="keyword" -a sweep_days=30 -a sweep_window_days=3

scrapy crawl facebook_comments -a post_id="<post id>" -a max_pages=3
```

Output is written to `output/<spider_name>_<timestamp>.json` by default (see
`social_crawler/settings.py`).

Run `python -m social_crawler.spiders.facebook.auth.bootstrap --help` or read
each spider's module docstring (`features/search/search.py`,
`features/comments/comments.py`) for the full list of arguments.

## Kafka-driven crawls (production)

`social_crawler/crawl_request_consumer.py` is a long-running process that
subscribes to Kafka's `crawl_requests` topic and, for each message, either
runs `scrapy crawl facebook_search` with that message's query/keyword_id/
date-range args, or (for `{"type": "refresh_token"}` messages) runs the
token-refresh bootstrap. Requests are handled one at a time - see the
module docstring for why.

Nothing publishes to `crawl_requests` from inside this repo - that's
`cinemark-api`'s job (`POST /facebook/run`, called manually or by its own
`scripts/trigger_scheduled_crawl.sh` cron, currently every 6h). This repo
only needs to be running the consumer and reachable to the same Kafka
broker:

```bash
python -m social_crawler.crawl_request_consumer
```

`deploy/systemd/spider-hub-crawl-consumer.service` runs this as a service
(`deploy/systemd/install.sh` installs it) - restarts on failure, and a
message that's mid-flight when the process dies isn't lost (its Kafka
offset isn't committed yet, so it's redelivered on restart).

## Other scheduled runs

- `scripts/refresh_token.sh` — headlessly refreshes the token cache before it
  expires (`CACHE_MAX_AGE_SECONDS`, 6h by default). Schedule every 4h - this
  one's still needed even with the Kafka consumer running (a
  `{"type": "refresh_token"}` message triggers the same script on demand,
  but the token still needs refreshing on its own schedule regardless of
  whether anyone happens to ask for a crawl).

```bash
crontab -e
# 0 */4 * * *  /path/to/spider-hub/scripts/refresh_token.sh >> /path/to/spider-hub/scripts/refresh_token.log 2>&1
```

`scripts/daily_run.sh` (a single hardcoded `scrapy crawl` on its own daily
cron) predates the Kafka-driven flow above and isn't part of the production
path anymore - every real keyword now goes through cinemark-api/D1 instead
of being hardcoded here. Left in place as a quick manual-testing example,
not something to schedule.

## Testing

```bash
pytest
```

Tests run against real captured Facebook response fixtures under
`tests/fixtures/` — if they start failing with fields silently coming back
`None`/empty, that's the signal Facebook's response schema drifted and the
field paths in `features/*/extract.py` need to be re-checked.

## Project structure

```
social_crawler/
  crawl_request_consumer.py             Kafka consumer - production entry point, see above    (shared)
  settings.py                          Scrapy settings + proxy/account/Telegram env vars    (shared)
  env.py                                Loads .env exactly once, however many modules import it (shared)
  logger.py                             structlog setup + Telegram alert forwarding          (shared)
  services/                                                                                  (shared)
    redis.py                            RedisCache - JSON get/set, atomic incr, sets
    kafka.py                            KafkaPublisher - posts/comments -> raw_posts/raw_comments topics
    telegram.py                         Telegram Bot API push
  constants/
    facebook.py                         Facebook-only: Redis keys, retry/pacing tuning, UI selectors
  spiders/
    facebook/                           First platform integration - see below
      items.py                          FacebookPostItem / FacebookEntityItem / FacebookCommentItem
      response_utils.py                 Shared dict/list tree-walk helpers for GraphQL responses
      auth/
        bootstrap.py                    CLI entry: browser login + GraphQL request capture
        accounts.py                     Parses FACEBOOK_ACCOUNTS, atomic account rotation
        cookies.py                      Cookie header parsing, storage_state building, cookie import
        triggers.py                     Playwright flows: login form, search/comments page actions
        browser_interaction.py          Generic Playwright helpers (human-like typing/mouse, selector fallback)
        request_capture.py              Captures + names the real GraphQL requests fired by a trigger
        graphql_client.py               Replays cached requests over curl_cffi; retry/backoff; pagination
      features/
        search/{search.py,extract.py}     facebook_search spider + response parsing
        comments/{comments.py,extract.py} facebook_comments spider + response parsing
    <next-platform>/                    Same shape: items.py, features/<feature>/, its own auth/ if needed
tests/                                   pytest suite against real response fixtures
scripts/                                 Cron entry points (refresh_token.sh; daily_run.sh - legacy, see above)
deploy/systemd/                          crawl_request_consumer.py as a systemd service
```

## Adding a new platform

The `facebook/` package under `spiders/` isn't special-cased anywhere outside
itself - `constants/facebook.py`, `spiders/facebook/auth/`, and
`spiders/facebook/features/` are all Facebook-only modules, while
`settings.py`, `env.py`, `logger.py` and `services/` are already
platform-agnostic and meant to be reused as-is. To add a platform:

1. Create `social_crawler/spiders/<platform>/` with its own `items.py` and a
   `features/<feature>/` subpackage per spider (mirroring `facebook/search`,
   `facebook/comments`).
2. Only build an `auth/` submodule if the platform actually needs
   login/session handling - many won't. If it's served through a paid API
   reseller (see the sibling `cinemark-scraper` project's TikTok/Threads/
   Facebook integrations for this shape), a spider can call that API
   directly with `requests`/`curl_cffi` and skip Playwright entirely.
3. Reuse `RedisCache` (`services/redis.py`) for any cross-run
   state - dedupe sets, rate-limit counters, rotation indexes - and
   `get_logger()` (`logger.py`) for logging, so Telegram alerting and
   platform-tagged console output come for free.
4. If a new platform ends up needing browser automation too,
   `browser_interaction.py`'s generic helpers (human-like typing/mouse,
   selector-fallback clicking) are candidates to promote out of
   `spiders/facebook/auth/` into a shared location rather than duplicating
   them - they don't know anything Facebook-specific today.
