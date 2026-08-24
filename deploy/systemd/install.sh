#!/bin/bash
# Installs and starts the crawl-consumer systemd service. Run on the
# server, as root/sudo, after the repo is deployed to /opt/spider-hub (or
# after editing this file's WorkingDirectory/ExecStart/EnvironmentFile
# paths to match wherever it actually lives).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

sudo cp "$SCRIPT_DIR/spider-hub-crawl-consumer.service" /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now spider-hub-crawl-consumer

echo "Installed. Check status with:"
echo "  systemctl status spider-hub-crawl-consumer"
echo "  journalctl -u spider-hub-crawl-consumer -f"
