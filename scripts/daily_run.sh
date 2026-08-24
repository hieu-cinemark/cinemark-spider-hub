#!/bin/bash
# Daily scheduled entry point: refreshes the Facebook session token first,
# then runs every crawl job below in order, reusing that fresh token - only
# ONE crontab line is needed no matter how many crawl jobs get added here.
#
# Install once via `crontab -e`:
#   0 7 * * * /Users/mypc/spider-hub/scripts/daily_run.sh >> /Users/mypc/spider-hub/scripts/daily_run.log 2>&1
#
# To add a new daily job: add another `scrapy crawl ...` line below. Each
# job is independent - one failing logs an error but doesn't block the rest.

set -uo pipefail  # no -e: one crawl job failing shouldn't skip the others

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
source .venv/bin/activate

log() { echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] $*"; }

log "=== Daily run starting ==="

if ! "$REPO_DIR/scripts/refresh_token.sh"; then
    log "Token refresh FAILED - skipping crawl jobs (would run with a stale/missing token)."
    exit 1
fi

# Tracks whether any job below failed, so the script's own exit code reflects
# it - without this, `|| log ...` swallows each job's exit code and the
# script always exits 0, so cron-level monitoring (mail-on-failure,
# dead-man's-switch tooling keyed on exit code) can never detect a failed run.
failed=0

# --- Crawl jobs: add more `scrapy crawl ...` lines below as needed ---

log "Running search crawl..."
if ! scrapy crawl facebook_search -a query="hộ linh tráng sĩ" -a count=10 -a max_pages=3; then
    log "search crawl FAILED (continuing with remaining jobs)"
    failed=1
fi

# log "Running comments crawl..."
# if ! scrapy crawl facebook_comments -a post_id="<post id>" -a max_pages=3; then
#     log "comments crawl FAILED (continuing with remaining jobs)"
#     failed=1
# fi

log "=== Daily run finished ==="
exit "$failed"
