#!/bin/bash
# Chạy consumer Facebook (có tài khoản) trên Mac, nối Kafka/Redis của VPS qua SSH. Xem docs/facebook-local.md.
# Trước khi chạy lần đầu: đặt CRAWLER_PLATFORMS=facebook_public,threads,tiktok trên VPS.
set -euo pipefail

VPS="${VPS_SSH:-root@160.191.54.31}"
VPS_PORT="${VPS_SSH_PORT:-7076}"
LOCAL_KAFKA_CONTAINER="${LOCAL_KAFKA_CONTAINER:-cinemark-workspace-kafka-1}"
REDIS_LOCAL_PORT=16379

cd "$(dirname "$0")/.."

# Kafka VPS quảng bá địa chỉ localhost:9092, nên tunnel phải đúng cổng 9092. Để Kafka local chạy thì tin nhắn đi nhầm vào đó.
if docker ps --format '{{.Names}}' | grep -qx "$LOCAL_KAFKA_CONTAINER"; then
  echo "Dừng Kafka local ($LOCAL_KAFKA_CONTAINER) để nhường cổng 9092"
  docker stop "$LOCAL_KAFKA_CONTAINER" >/dev/null
fi

ssh -N -p "$VPS_PORT" \
  -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
  -L "127.0.0.1:9092:127.0.0.1:9092" -L "127.0.0.1:$REDIS_LOCAL_PORT:127.0.0.1:6379" \
  "$VPS" &
TUNNEL_PID=$!
trap 'kill "$TUNNEL_PID" 2>/dev/null || true' EXIT
sleep 4
kill -0 "$TUNNEL_PID" 2>/dev/null || { echo "Không mở được tunnel SSH"; exit 1; }

export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export REDIS_HOST=localhost REDIS_PORT="$REDIS_LOCAL_PORT"
export CRAWLER_PLATFORMS=facebook
export CONSUMER_LOG_FILE="${CONSUMER_LOG_FILE:-consumer-facebook-local.log}"

.venv/bin/python -m scripts.check_facebook_local
exec_status=0
.venv/bin/python -m social_crawler.consumer.main || exec_status=$?
exit "$exec_status"
