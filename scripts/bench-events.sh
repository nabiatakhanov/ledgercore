#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
requests=${1:-256}; repeats=${2:-3}
case "$requests:$repeats" in *[!0-9:]*|:*|*:) echo 'Requests must be 32..100000 and Repeats must be 2..10' >&2; exit 1;; esac
if ! [ "$requests" -ge 32 ] || ! [ "$requests" -le 100000 ] || ! [ "$repeats" -ge 2 ] || ! [ "$repeats" -le 10 ]; then
    echo 'Requests must be 32..100000 and Repeats must be 2..10' >&2
    exit 1
fi
run_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
run_dir="$(pwd)/.runtime/events-bench/$run_id"
helper=scripts/events-report.py
python3 "$helper" init "$run_dir" bench "$run_id"
old_limit_set=${RATE_LIMIT+x}; old_limit=${RATE_LIMIT-}
old_window_set=${RATE_WINDOW+x}; old_window=${RATE_WINDOW-}
run_docker() { label=$1; seconds=$2; shift 2; python3 "$helper" run "$run_dir" "$label" "$seconds" docker "$@"; }
cleanup() {
    result=$?
    trap - EXIT INT TERM
    if [ "$old_limit_set" = x ]; then export RATE_LIMIT="$old_limit"; else unset RATE_LIMIT; fi
    if [ "$old_window_set" = x ]; then export RATE_WINDOW="$old_window"; else unset RATE_WINDOW; fi
    if ! run_docker restore-original-gateways 90 compose up -d --no-deps --force-recreate --wait --wait-timeout 60 gateway gateway2 >/dev/null; then result=1; fi
    status=failed; message='Benchmark or original Gateway restore failed; inspect raw logs'
    if [ "$result" -eq 0 ]; then status=passed; message=''; fi
    python3 "$helper" finish "$run_dir" "$status" "$message"
    echo "Benchmark report: $(dirname "$run_dir")/report.json"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
export RATE_LIMIT=1000000 RATE_WINDOW=1m
python3 "$helper" config "$run_dir" >/dev/null
source_sha=$(git rev-parse HEAD)
run_docker up 900 compose up --build -d --wait --wait-timeout 180 >/dev/null
run_docker benchmark-gateways 90 compose up -d --no-deps --force-recreate --wait --wait-timeout 60 gateway gateway2 >/dev/null
hardware=$(run_docker docker-hardware 30 info --format '{"server_version":"{{.ServerVersion}}","cpu_count":{{.NCPU}},"memory_bytes":{{.MemTotal}}}')
python3 "$helper" host "$run_dir" host "$hardware"
run_docker images 30 compose images --format json >/dev/null
ids_file=$(run_docker stats-before-ids 30 compose ps -q)
run_docker stats-before 30 stats --no-stream --format '{{json .}}' $(cat "$ids_file") >/dev/null
data=$(run_docker events-bench 660 compose --profile tools run --rm --no-deps \
    -e GATEWAY_URLS=http://gateway:8080,http://gateway2:8080 -e RATE_LIMIT=1000000 -e RATE_WINDOW=1m \
    -e "SOURCE_COMMIT_SHA=$source_sha" ledgerctl events-bench -n "$requests" -repeats "$repeats")
python3 "$helper" bench "$run_dir" bench "$data"
ids_file=$(run_docker stats-after-ids 30 compose ps -q)
run_docker stats-after 30 stats --no-stream --format '{{json .}}' $(cat "$ids_file") >/dev/null
verify=$(run_docker phase-verify 660 compose --profile tools run --rm --no-deps ledgerctl events-demo --phase verify)
python3 "$helper" phase "$run_dir" verify "$verify"
