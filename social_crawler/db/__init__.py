"""Postgres (Supabase) access for account/proxy config - these change often
enough (accounts get disabled/swapped, proxies get rotated) that editing
.env and restarting every process that reads it (bootstrap.py's subprocess,
`scrapy crawl`'s subprocess, crawl_request_consumer.py's long-lived service)
stopped being acceptable. A change to the platform_accounts/platform_proxies
tables takes effect on the very next call, no restart needed.

A fresh connection per call, not a pool: every caller here either runs
inside a short-lived subprocess (bootstrap.py runs once per refresh, a
`scrapy crawl` process runs once per crawl) or calls this rarely enough
(once at crawl/session start) that pool lifecycle management would add
complexity for no real benefit.

One module per table (or group of tables), no ORM/migration tool:

  connection.py      connect() - the one place DATABASE_URL/LOCAL_DATABASE_URL
                     is resolved
  accounts.py        platform_accounts(id, platform, account_id, password,
                     totp_secret, cookie, token, email, email_password,
                     enabled, created_at, updated_at, ...)
  proxies.py         platform_proxies(id, platform ['all' = shared across
                     every platform], proxy_url, username, password,
                     login_use_proxy, enabled, created_at, updated_at, ...)
                     + platform_accounts.assigned_proxy_id pinning
  relogin.py         the auto-login flow's slice of platform_accounts (dead
                     cookies, needs_manual_login, last_relogin_*)
  config.py          filter_keywords, ai_providers, ai_settings - CRUD'd
                     from the dashboard (cinemark-api's
                     app/services/platform_config_db.py), read-only here
  proxy_settings.py  proxy_settings + proxy_providers, cached, with
                     DEFAULTS when the row/table is missing
"""
