#!/bin/bash
set -eu
port="${HEALTH_PORT:-8080}"
case "$port" in ''|*[!0-9]*) exit 1 ;; esac
exec 3<>/dev/tcp/127.0.0.1/"$port"
printf 'GET /readyz HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n' >&3
IFS= read -r -t 3 status <&3
case "$status" in
  'HTTP/1.0 200 '*|'HTTP/1.1 200 '*) exit 0 ;;
  *) exit 1 ;;
esac
