#!/bin/bash
# Serve the spider-hub dashboard on a domain that already has a Let's Encrypt certificate (certbot), behind the same
# Basic Auth as install.sh. Run as root AFTER install.sh:  bash install-domain.sh spider-hub.id.vn
# Backs up and disables the old certbot-managed site for that domain (the one without a password) and the IP:8443
# fallback.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
DOMAIN="${1:?usage: install-domain.sh <domain>}"
test -f "/etc/letsencrypt/live/${DOMAIN}/fullchain.pem" || { echo "no certificate for ${DOMAIN} - run certbot first"; exit 1; }
test -f /etc/nginx/spider-hub.htpasswd || { echo "no /etc/nginx/spider-hub.htpasswd - run install.sh first"; exit 1; }

cp nginx-upgrade-map.conf /etc/nginx/conf.d/spider-hub-upgrade-map.conf
cp spider-hub-locations.conf /etc/nginx/snippets/spider-hub-locations.conf

BACKUP=/root/nginx-backup-$(date +%Y%m%d-%H%M%S)
mkdir -p "$BACKUP"
for site in /etc/nginx/sites-enabled/*; do
  [ -e "$site" ] || continue
  name=$(basename "$site")
  # Old site serving the same domain, and the IP:8443 fallback - both replaced by the domain site below.
  if [ "$name" = "spider-hub" ] || grep -q "server_name[^;]*\b${DOMAIN}\b" "$site"; then
    cp -L "$site" "$BACKUP/$name"
    rm "$site"
    echo "disabled $name (backup in $BACKUP)"
  fi
done

sed "s/__DOMAIN__/${DOMAIN}/g" nginx-spider-hub-domain.conf > /etc/nginx/sites-available/spider-hub-domain
ln -sf /etc/nginx/sites-available/spider-hub-domain /etc/nginx/sites-enabled/spider-hub-domain
if ! nginx -t; then
  echo "nginx config test failed - restoring $BACKUP"; rm -f /etc/nginx/sites-enabled/spider-hub-domain
  cp "$BACKUP"/* /etc/nginx/sites-enabled/ 2>/dev/null || true; exit 1
fi
systemctl reload nginx
if command -v ufw >/dev/null && ufw status | grep -q "Status: active"; then
  ufw allow 80/tcp; ufw allow 443/tcp; ufw delete allow 8443/tcp || true
fi
echo "OK: https://${DOMAIN} (Basic Auth). Rebuild the dashboard with NEXT_PUBLIC_API_BASE_URL=https://${DOMAIN}/api."
