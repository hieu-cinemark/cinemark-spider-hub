#!/bin/bash
# Refreshes the Facebook session token cache in Redis before it expires
# (CACHE_MAX_AGE_SECONDS, currently 6h). Runs fully headless and
# non-interactively - it reuses the storage_state already cached in Redis,
# no login prompt.
#
# Scheduled on its own (every 4h, comfortably inside the 6h TTL) so the
# token stays fresh all day for ad-hoc `scrapy crawl` runs, not just right
# after daily_run.sh's 7am batch - which also calls this same script before
# its crawl jobs, so the two schedules just overlap harmlessly.
#
#   0 */4 * * * /Users/mypc/spider-hub/scripts/refresh_token.sh >> /Users/mypc/spider-hub/scripts/refresh_token.log 2>&1
#
# Run `crontab -e` and add that line (adjust the paths if this repo moves).

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
source .venv/bin/activate

echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Refreshing Facebook token cache..."
# Use a natural search phrase, not an arbitrary string - Facebook only fires
# the real results query when it actually has results to return, and
# bootstrap.py now fails loudly (no Redis write) rather than silently
# caching the wrong query if this doesn't come back with real results.
python -m social_crawler.spiders.facebook.auth.bootstrap --query "tin tức hôm nay"
echo "[$(date -u +"%Y-%m-%d %H:%M:%S UTC")] Done."
