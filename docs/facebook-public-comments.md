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

## Evidence and limits — 03/11/2026

- A local anonymous probe through unassigned proxy #10 returned **4 top-level comments and 1 depth-1 reply**, in two requests, from the public post used in upstream's example. No account or Kafka publishing was involved. Both connections ended without another page in this sample.
- Proxy #15 was rejected because it is assigned to an account. Its credentials were not used for a Facebook request.
- Upstream README reports 2,817 comments on 20 posts and 93.5% weighted coverage. It also says the anonymous/authenticated runs used different days/posts. The linked `reports/ANONYMOUS_ACCESS_SOLVED_2026-07-28.md` is absent from this checkout. That benchmark has not been independently reproduced here.
- The sample establishes that these payloads/Doc IDs worked for that public post at test time. It does not establish coverage for all film posts or every comment/reply type, nor guaranteed absence of IP restrictions.
