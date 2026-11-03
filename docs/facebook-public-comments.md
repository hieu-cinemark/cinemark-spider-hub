# Facebook public comments, based on fbgql

Source reviewed: [bsho5/fbgql](https://github.com/bsho5/fbgql/tree/28e03ae84012be44d37a67014fcfcfcc420c9ee9), commit `28e03ae84012be44d37a67014fcfcfcc420c9ee9`. Adapted payloads/provider variables and shipped comment/reply Doc IDs retain the [MIT notice](../licenses/fbgql-MIT.txt). No new library dependency or login helper is installed.

## Integration

New Facebook comment jobs published by cinemark-api default to `access_mode=anonymous`, with depth-1 replies enabled. A dedicated topic `crawl_requests.facebook_public_comments` and Kafka group `spider-hub.crawl-requests.facebook-public-comments` process these jobs independently of account availability, account hourly caps and account lanes. The running job uses `crawl_job:facebook:public`; API job listing and Stop include that key. Stop/drain/cancellation still use the existing Facebook namespace.

The new `facebook_public_comments` spider sends the same `raw_comments` records as existing spiders, including stable GraphQL comment IDs, the database post external ID and `parent_comment_id` for replies. Existing Kafka ingest/retry/DLQ/D1 code remains the persistence path. A broker acknowledgement is required before marking an item seen. Future jobs may refresh the same comment; there is no permanent dedupe that could hide updates.

Every request explicitly uses actor `0` and an empty token field, with an independent HTTP session. No account is allocated, no account cookie/token is loaded, no browser is started, and cookies are cleared before every POST. The proxy must be enabled and its endpoint must not be assigned to any account, including disabled accounts; this is checked before each request. Proxy rotation and authenticated fallback are not used.

Explicit `access_mode=authenticated` and legacy queued messages without a mode retain their account-based path. A failed authenticated session is never silently converted into anonymous mode. Public-only access can fail for unavailable/gated content; that failure does not touch an account.

## Pagination and error behavior

- Comments use `POST_PERMALINK_DIALOG` with `REVERSE_CHRONOLOGICAL_UNFILTERED_INTENT_V1`.
- Replies use the **feedback ID and expansion token returned by the server**, not an inferred comment feedback ID. The depth-1 connection is paginated; nested depth-2 threads are outside this integration's coverage.
- Empty critical `1675012` pages retry the same cursor at most twice after the first attempt, with backoff. Persistent failure is incomplete, not a successful zero-comment result.
- Unknown response structure, repeated cursor, missing reply metadata for a known reply count, exhausted request budget, critical errors and failed Kafka acknowledgement all fail visibly. Already published records are retained. A later job can retry.
- A positive `max_pages` is the job's chosen depth: reaching it logs `public_comments_page_limit_reached` / `public_replies_page_limit_reached` and the job ends as done, so scheduled posts are not requeued from page 1 every round. That job did not necessarily fetch every comment. Use `max_pages=0` for a full crawl.
- While the public proxy is unusable, in cooldown, or out of daily budget, the worker holds its queue (`public_comments_queue_held`) instead of failing each job. An unusable proxy also alerts Telegram. A stale Doc ID or changed schema sets a 30-minute cooldown, so at most one job fails per cooldown until it is fixed.
- HTTP 403/429 stop immediately and put the public proxy in a 30-minute cooldown. Transient network/5xx failures retry at most twice; repeated failure puts it in a short cooldown. Daily/job budgets count every physical attempt.
- IDs are checked by actual GraphQL response structure/errors. Stale IDs and changed variable schemas are reported separately. Successful/stale validation is recorded per operation in Redis; no browser/account is used to repair it.

## Configuration

```dotenv
FACEBOOK_PUBLIC_COMMENTS_PROXY_ID=10
FACEBOOK_PUBLIC_COMMENTS_INTERVAL=5
FACEBOOK_PUBLIC_COMMENTS_DAILY_CAP=400
FACEBOOK_PUBLIC_COMMENTS_MAX_REQUESTS=400
# Optional overrides, only when the shipped query has changed:
# FBGQL_DOC_ID_COMMENTS=<numeric-id>
# FBGQL_DOC_ID_REPLIES=<numeric-id>
```

These are resource budgets, not thresholds proven safe. Resolution priority for Doc IDs is environment override, Redis `facebook:public_comment_doc_ids` (`comments`/`replies`), publicly discovered operation IDs, then the pinned upstream defaults. A new ID may also require new variables; a schema error must be fixed in code.

The API keeps its existing page limit of 80 unless specified. For anonymous jobs, `max_pages=0` removes the page cap, while request budgets still apply:

```text
POST /facebook/posts/{database-post-id}/comments/run?access_mode=anonymous&max_pages=0&include_replies=true
```

Use the existing API authentication. Authenticated mode requires a positive page limit. Threads/TikTok publishers retain their previous payload contract.

## Local commands

Doctor: at most one comment page, and optionally one eligible reply page. Prints counts/validation only, sends nothing to Kafka:

```sh
.venv/bin/python -m social_crawler.spiders.facebook.auth.public_comments \
  --post-id 122216361518376966 --proxy-id 10 --check-replies
```

Scrape through the normal persistence pipeline (this **does publish** to Kafka):

```sh
.venv/bin/scrapy crawl facebook_public_comments \
  -a post_id='<numeric-or-database-composite-post-id>' -a max_pages=0
```

Deploy spider-hub before cinemark-api so the anonymous worker exists before new-mode jobs are published. If broker auto-creation is disabled, create `crawl_requests.facebook_public_comments` with the cluster's existing replication configuration. The separate topic prevents old workers from receiving an anonymous job and using an account by mistake. Existing topics, consumer groups and offsets are unchanged; retained account-based jobs stay on their original path.

## Logged-out page reads

Proxies labelled `platform=public` (#10 for comments, #15 first for page reads) are never picked when an account is auto-pinned, because the pool only selects its own platform or `all`.

The scheduler keeps the queue `crawl_public_pages:facebook` filled to `FACEBOOK_PUBLIC_PAGES_QUEUE_TARGET` (default 6), regardless of the Facebook crawl schedule switch. Pages past their source schedule go first, then any page not queued in the last `FACEBOOK_PUBLIC_PAGES_MIN_GAP_MINUTES` (default 60). After a complete read, the next job only asks for posts since that read plus one day, not the full window. Dedicated workers in the crawl consumer run jobs in parallel: `FB_PUBLIC_PAGES_PER_PROXY` (default 1) per proxy in `FB_PUBLIC_PAGES_PROXY_IDS`, so two by default. The `public_page` job saves page info to Redis `facebook:public_page_info:<url>` and sends new posts to `raw_posts` in the `page_feed` format.

The default proxy list is `15,10`. An endpoint is eligible only when enabled, outside DB and public-page cooldowns, marked `platform='public'`, and not assigned to **any** account (including disabled accounts and accounts assigned through another row with the same endpoint URL). An accidental account-proxy ID in the allowlist therefore stays unusable. Full or cooling-down proxies are skipped; when all eligible endpoints are busy/unavailable the workers leave queued jobs pending and poll again. If a proxy slot becomes busy between the preflight check and job start, only that claimed job is atomically returned to the queue, without marking it completed or disturbing another worker's claim. After a job finishes, its worker takes the next queued page. A recovered endpoint clears the resource-pause flag immediately, while an explicit platform stop/drain remains respected.

The initial HTML usually embeds one post with a deferred timeline cursor. The job now keeps an isolated guest cookie jar and the LSD token from that HTML, discovers `ProfileCometTimelineFeedRefetchQuery` in the page's public JavaScript assets, and follows the feed cursor over HTTP. Asset JavaScript is read, never executed. Requests always use actor/user `0`, with no account token, browser or authenticated fallback. The public Doc ID is cached by server revision for 24 hours; an explicit stale-document error clears that cache. `FB_PUBLIC_PAGES_DOC_ID` can override discovery.

The default scope is the **last 60 days**, measured once at the start of each job (`FB_PUBLIC_PAGES_SINCE_DAYS=60`, overridable with `since_days`). The refetch query receives `afterTime`/`beforeTime` and starts with `cursor=null`: a cursor from the unfiltered HTML belongs to a different connection and must not be reused with date filters. Both HTML and GraphQL posts are also filtered locally by their publication timestamp, including the 60-day boundary; older/future posts and posts without a timestamp are excluded. An old pinned HTML post never stops the filtered query.

`FB_PUBLIC_PAGES_MAX_PAGES` now defaults to **0**: no page limit within this date window. A job's `max_pages` can impose an explicit limit (including the initial HTML page). When upgrading, remove any existing `FB_PUBLIC_PAGES_MAX_PAGES=10` or set it to `0` to scan the entire window. Requests are spaced at least `FB_PUBLIC_PAGES_REQUEST_INTERVAL` seconds (default **5**). The server may return fewer than the requested 10 posts. The crawl stops on exhaustion, a batch entirely older than the date boundary, repeated/missing cursor, no new posts, login/checkpoint redirect, HTTP failure or changed response schema. Already collected posts remain publishable after a pagination failure, while the block still counts toward proxy cooldown. The result and `page_feed_last:<url>` include `pages_fetched`, `stop_reason`, `pagination_status`, `since_ts` and `until_ts`, so the date window and a partial crawl are visible.

On 03/11/2026, the integrated collector through proxy #17 returned **13 distinct posts across 5 pages** from `traibuonnguoi.movie.vn` in 32 seconds, stopping at the test's page limit with no pagination error. No account or Kafka publishing was involved. The previous bare anonymous POST's 302 did not establish that guest feed pagination was unavailable: the guest session and HTML token are required in the working request. This is a limited test, not a guarantee of full page history or a safe sustained request rate.

A subsequent run with the 60-day window and no page cap returned **475 distinct posts across 160 pages** (including HTML), ending with `has_next_page=false` after about 18 minutes. Returned publication dates ranged from 14/09/2026 to 30/10/2026; all saved posts were inside the requested 04/09–03/11 window. No pagination error occurred. This records the endpoint's returned coverage for this guest session, not independent proof that every public post in that window was returned. Posts and the sanitized report were saved locally, without Kafka/DB ingestion.

On 03/11/2026, about 45 GETs spaced 8-14 seconds apart got proxy #10 redirected to login for over 40 minutes, while anonymous comment GraphQL on the same proxy kept working. The default of one job per proxy is a starting point, not a proven safe rate. After 3 blocked reads in a row a proxy rests for 120 minutes; with no usable proxy the scheduler pauses for 10 minutes.

Explicit HTTP 429 or GraphQL restriction signals (including `1675004: Rate limit exceeded` in an HTTP 200 body) now rest the proxy immediately for the configured cooldown. They are not schema/Doc ID failures and never trigger another query to confirm the restriction. A later live check found #15 could still serve the initial HTML while its feed GraphQL returned `1675004`; #10 redirected the page to login. Both public-page endpoints were therefore placed in cooldown, without resetting IPs or borrowing account endpoints. Continuous workers wait when the entire pool is restricted and resume eligible jobs after cooldown; uninterrupted upstream access is not guaranteed.

A blocked proxy records its exit IP. While it cools down, a worker re-measures the exit IP at most once per `FB_PUBLIC_PAGES_IP_CHECK_SECONDS` (default 60, via `api.ipify.org`, not Facebook). A new IP that has not itself been blocked in the cooldown window ends the cooldown at once, so rotating proxies resume in about a minute. An unchanged IP, such as a static proxy, still rests the full cooldown. This assumes a block belongs to the IP; a new IP from the same provider range can be blocked again, which simply starts another cooldown.

Scheduled Facebook comment rounds no longer depend on the crawl schedule switch, because those jobs are anonymous. Set `FACEBOOK_PUBLIC_PAGES_ENABLED=false` on cinemark-api to stop page reads; comment rounds are controlled by the comment schedule.

## Evidence and limits — 03/11/2026

- A local anonymous probe through unassigned proxy #10 returned **4 top-level comments and 1 depth-1 reply**, in two requests, from the public post used in upstream's example. No account or Kafka publishing was involved. Both connections ended without another page in this sample.
- Proxy #15 was rejected because it is assigned to an account. Its credentials were not used for a Facebook request.
- Upstream README reports 2,817 comments on 20 posts and 93.5% weighted coverage. It also says the anonymous/authenticated runs used different days/posts. The linked `reports/ANONYMOUS_ACCESS_SOLVED_2026-07-28.md` is absent from this checkout. That benchmark has not been independently reproduced here.
- The sample establishes that these payloads/Doc IDs worked for that public post at test time. It does not establish coverage for all film posts or every comment/reply type, nor guaranteed absence of IP restrictions.
