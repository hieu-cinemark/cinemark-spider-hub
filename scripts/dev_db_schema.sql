-- Local dev copy of the platform_accounts / platform_proxies tables
-- (production is Supabase Postgres - see social_crawler/services/db.py).
-- Mirrors prod's real columns (confirmed against cinemark-api's
-- ACCOUNT_COLUMNS/PROXY_COLUMNS in app/services/platform_config_db.py) plus
-- new account/proxy pool columns (status, cooldown_until,
-- consecutive_failures, last_used_at, assigned_proxy_id) being tried out
-- here first, before migrating prod - see social_crawler/services/pool.py
-- for how they're used.
--
-- Apply once against the local dev Postgres (see docker-compose.yml at the
-- workspace root, "postgres" service):
--   docker compose up -d postgres
--   docker compose exec -T postgres psql -U postgres -d spider_hub_dev < scripts/dev_db_schema.sql

CREATE TABLE IF NOT EXISTS platform_accounts (
    id serial PRIMARY KEY,
    platform text NOT NULL,
    account_id text NOT NULL,
    password text,
    totp_secret text,
    cookie text,
    token text,
    email text,
    email_password text,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    last_checked_at timestamptz,
    last_check_status text,
    -- Human-readable note for last_check_status/last_checked_at - already
    -- existed on prod before this session (predates the pool columns
    -- below), used by the dashboard's health-check flow. Also now written
    -- by services/kira.diagnose_account_failure whenever record_account_
    -- outcome/disable_account hard-disables an account, so a checkpoint
    -- carries a short AI-generated diagnosis instead of just a raw log line.
    last_check_note text,
    -- account pool / circuit-breaker columns - see services/pool.py
    status text NOT NULL DEFAULT 'active',
    cooldown_until timestamptz,
    consecutive_failures int NOT NULL DEFAULT 0,
    last_used_at timestamptz,
    -- Sticky proxy pinning (see services/pool.acquire_proxy_for_account) -
    -- once an account is first used, it's permanently paired with one
    -- proxy row so the same account always looks like it's coming from the
    -- same IP, instead of a pool of accounts fanning out across every
    -- proxy at random (a strong multi-accounting signal on FB/Threads/
    -- TikTok). NULL until that first use. The FK itself is added below,
    -- after platform_proxies exists.
    assigned_proxy_id int,
    UNIQUE (platform, account_id)
);

CREATE TABLE IF NOT EXISTS platform_proxies (
    id serial PRIMARY KEY,
    platform text NOT NULL,
    proxy_url text NOT NULL,
    username text,
    password text,
    login_use_proxy boolean NOT NULL DEFAULT false,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    -- proxy pool / circuit-breaker columns - see services/pool.py
    status text NOT NULL DEFAULT 'active',
    cooldown_until timestamptz,
    consecutive_failures int NOT NULL DEFAULT 0,
    last_used_at timestamptz,
    UNIQUE (platform, proxy_url)
);

-- Every ALTER below is safe to re-run against a table that already existed
-- before these columns were added (CREATE TABLE IF NOT EXISTS above is a
-- no-op in that case, so each column needs its own migration path here) as
-- well as against a brand-new table (already has the column from CREATE
-- TABLE above, ADD COLUMN IF NOT EXISTS then just no-ops). This is exactly
-- what running this file against prod Supabase does - see module docstring.
ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS last_check_note text;
ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active';
ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS cooldown_until timestamptz;
ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0;
ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS last_used_at timestamptz;
ALTER TABLE platform_accounts ADD COLUMN IF NOT EXISTS assigned_proxy_id int;

ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'active';
ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS cooldown_until timestamptz;
ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS consecutive_failures int NOT NULL DEFAULT 0;
ALTER TABLE platform_proxies ADD COLUMN IF NOT EXISTS last_used_at timestamptz;

-- Added after both tables exist - see platform_accounts.assigned_proxy_id
-- above. ON DELETE SET NULL so removing a proxy row just makes its pinned
-- accounts eligible for re-pinning on their next run, rather than failing
-- the delete.
ALTER TABLE platform_accounts
    DROP CONSTRAINT IF EXISTS platform_accounts_assigned_proxy_id_fkey;
ALTER TABLE platform_accounts
    ADD CONSTRAINT platform_accounts_assigned_proxy_id_fkey
    FOREIGN KEY (assigned_proxy_id) REFERENCES platform_proxies (id) ON DELETE SET NULL;

-- Generic content-filter keywords (movie-relevant vs spam/off-topic) -
-- CRUD'd from the dashboard (cinemark-api's /settings/filter-keywords, see
-- app/services/platform_config_db.py there), read by spider-hub to decide
-- whether a scraped post/comment is worth keeping. Already created on prod
-- Supabase directly (see chat history) - this is just the local dev mirror.
CREATE TABLE IF NOT EXISTS filter_keywords (
    id serial PRIMARY KEY,
    keyword text NOT NULL,
    category text NOT NULL CHECK (category IN ('movie_relevant', 'spam_offtopic')),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (category, keyword)
);

-- Per-platform daily crawl schedule - cinemark-api-owned (not read by
-- spider-hub at all, just living in the same shared Postgres by
-- convention), CRUD'd from the dashboard's new "Crawl schedule" card (see
-- cinemark-api's app/services/platform_config_db.py + app/services/
-- scheduler.py). Replaces both cinemark-api's own OS-crontab-driven
-- scripts/trigger_scheduled_crawl.sh (which used to trigger every platform
-- on one fixed "every 6h" cadence with no way to change it without editing
-- a crontab) and cinemark-scraper's Cloudflare Cron Triggers for Threads/
-- TikTok (wrangler.toml) - going forward, cinemark-api's own in-process
-- scheduler is the one source of truth for when a platform's crawl runs,
-- editable from the dashboard with no crontab/Worker redeploy needed.
CREATE TABLE IF NOT EXISTS crawl_schedules (
    platform text PRIMARY KEY,
    -- "HH:MM", interpreted in a fixed Asia/Ho_Chi_Minh timezone by
    -- app/services/scheduler.py - not stored as a real time/timestamptz
    -- since there's no per-row timezone need, just a single operator's
    -- local daily schedule.
    run_time text NOT NULL DEFAULT '07:00',
    enabled boolean NOT NULL DEFAULT true,
    -- scheduler.py's own re-entrancy guard: the date (YYYY-MM-DD, same
    -- Asia/Ho_Chi_Minh clock) this platform's schedule last actually fired
    -- on, so a poll loop checking every 30s doesn't fire the same
    -- scheduled run repeatedly during the same matching minute. NULL until
    -- the first time it ever fires.
    last_triggered_date date,
    -- Warm Facebook/Threads sessions on the same daily tick as the crawl
    -- (see cinemark-api scheduler._trigger_platform). TikTok has no
    -- nurture flow; those flags are ignored for it.
    nurture_before boolean NOT NULL DEFAULT false,
    nurture_after boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ai_settings (
    id integer PRIMARY KEY CHECK (id = 1),
    enabled boolean NOT NULL DEFAULT false,
    model text NOT NULL DEFAULT 'qwen3.8-flash',
    prompts jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Seed your own account/proxy locally (don't commit real credentials -
-- run these by hand instead, e.g. via `docker compose exec postgres psql ...`):
--
-- INSERT INTO platform_accounts (platform, account_id, password, totp_secret, email)
--   VALUES ('facebook', 'you@example.com', 'your-password', '', 'you@example.com');
--
-- INSERT INTO platform_proxies (platform, proxy_url, username, password, login_use_proxy)
--   VALUES ('facebook', '<proxy_host>:<proxy_port>', '<username>', '<password>', false);
