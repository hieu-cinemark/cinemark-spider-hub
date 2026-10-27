#!/bin/bash
# Manual-login display + public front for the spider-hub dashboard, on the VPS itself (no Cloudflare):
#   Xvfb :99 + x11vnc + noVNC (all 127.0.0.1) and nginx on :8443 (self-signed TLS; /novnc ticket-protected) serving the
#   dashboard, /api (cinemark-api) and /novnc. Run as root:  bash /opt/spider-hub/deploy/login-display/install.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
PUBLIC_IP="$(curl -s -4 https://api.ipify.org || hostname -I | awk '{print $1}')"

apt-get update
apt-get install -y xvfb x11vnc novnc websockify nginx apache2-utils
# System libraries for HEADED Chromium (auto-login only ever needed headless).
/opt/spider-hub/.venv/bin/python -m playwright install-deps chromium
sudo -u app /opt/spider-hub/.venv/bin/python -m playwright install chromium

cp spider-hub-login-xvfb.service spider-hub-login-vnc.service spider-hub-login-novnc.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now spider-hub-login-xvfb spider-hub-login-vnc spider-hub-login-novnc

# Self-signed certificate for the IP (no domain -> no Let's Encrypt). The browser warns once; accept it.
mkdir -p /etc/nginx/ssl
if [ ! -f /etc/nginx/ssl/spider-hub.crt ]; then
  openssl req -x509 -nodes -newkey rsa:2048 -days 825 \
    -keyout /etc/nginx/ssl/spider-hub.key -out /etc/nginx/ssl/spider-hub.crt \
    -subj "/CN=${PUBLIC_IP}" -addext "subjectAltName=IP:${PUBLIC_IP}"
fi

cp nginx-upgrade-map.conf /etc/nginx/conf.d/spider-hub-upgrade-map.conf
cp spider-hub-locations.conf /etc/nginx/snippets/spider-hub-locations.conf
cp nginx-spider-hub.conf /etc/nginx/sites-available/spider-hub
ln -sf /etc/nginx/sites-available/spider-hub /etc/nginx/sites-enabled/spider-hub
nginx -t && systemctl reload nginx
if command -v ufw >/dev/null && ufw status | grep -q "Status: active"; then ufw allow 8443/tcp; fi

echo "OK. Dashboard will be at https://${PUBLIC_IP}:8443 once spider-hub-dashboard is deployed (its deploy/README.md)."
echo "Have a domain with a certbot certificate? Then also run: bash install-domain.sh <domain>"
