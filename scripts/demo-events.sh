#!/bin/sh
# Docker/Git plus Python3 stdlib for reports; no jq/PyPI dependency.
set -eu
cd "$(dirname "$0")/.."
hold=${EVENTS_DEMO_DOWNTIME_SECONDS:-10}
case "$hold" in ''|*[!0-9]*) echo 'Downtime must be 1..60 seconds' >&2; exit 1;; esac
[ "$hold" -ge 1 ] && [ "$hold" -le 60 ]
run_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
run_dir="$(pwd)/.runtime/events-demo/$run_id"
helper=scripts/events-report.py
python3 "$helper" init "$run_dir" demo "$run_id"
restored=0
run_docker() { label=$1; seconds=$2; shift 2; python3 "$helper" run "$run_dir" "$label" "$seconds" docker "$@"; }
cleanup() {
    result=$?
    trap - EXIT INT TERM
    if [ "$result" -ne 0 ]; then
        run_docker failure-component-logs 30 compose logs --no-color --tail 200 ledger publisher activity gateway gateway2 >/dev/null || :
    fi
    if [ "$restored" -ne 1 ]; then
        if run_docker failure-restore 210 compose up -d --wait --wait-timeout 180 >/dev/null; then restored=1; else result=1; fi
    fi
    run_docker component-logs 30 compose logs --no-color --tail 200 ledger publisher activity gateway gateway2 >/dev/null || :
    status=failed; message='Demo command failed; inspect raw phase logs'
    if [ "$result" -eq 0 ]; then status=passed; message=''; fi
    python3 "$helper" finish "$run_dir" "$status" "$message" "$restored"
    echo "Demo report: $(dirname "$run_dir")/report.json"
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
set -- $(python3 "$helper" config "$run_dir")
rate_limit=$1; rate_window=$2
if [ "$rate_limit" != 60 ] || [ "$rate_window" != 1m ]; then echo 'Full demo expects RATE_LIMIT=60 and RATE_WINDOW=1m; individual CLI phases support other values' >&2; exit 1; fi
source_sha=$(git rev-parse HEAD)
phase() {
    name=$1; identity=${2:-$run_id}; count=${3:-20}
    echo "Demo phase: $name" >&2
    file=$(run_docker "phase-$name" 660 compose --profile tools run --rm --no-deps \
        -e GATEWAY_URLS=http://gateway:8080,http://gateway2:8080 -e "RATE_LIMIT=$rate_limit" -e "RATE_WINDOW=$rate_window" \
        -e "SOURCE_COMMIT_SHA=$source_sha" ledgerctl events-demo --phase "$name" --run-id "$identity" --count "$count")
    python3 "$helper" phase "$run_dir" "$name" "$file"
    printf '%s\n' "$file"
}
run_docker up 900 compose up --build -d --wait --wait-timeout 180 >/dev/null
hardware=$(run_docker docker-hardware 30 info --format '{"server_version":"{{.ServerVersion}}","cpu_count":{{.NCPU}},"memory_bytes":{{.MemTotal}}}')
python3 "$helper" host "$run_dir" host "$hardware"
run_docker images 30 compose images --format json >/dev/null
# Wait through the measured existing TTL with one request, reusing the final
# limiter burst key. Do not delete or reset another request's shared budget.
phase limiter "$run_id" 1 >/dev/null
phase normal >/dev/null
kafka_stopped=$(date +%s)
run_docker kafka-stop 30 compose stop -t 12 kafka >/dev/null
queued=$(phase enqueue "$run_id-kafka" 20)
python3 "$helper" backlog "$run_dir" "$queued" 20
# An explicit experimental hold; health/catch-up is checked by bounded CLI polling.
sleep "$hold"
phase status >/dev/null
run_docker publisher-stop-during-outage 30 compose stop -t 12 publisher >/dev/null
outage_publisher_file=$(run_docker outage-publisher-container 30 compose ps -a -q publisher)
outage_publisher=$(cat "$outage_publisher_file")
outage_exit_file=$(run_docker outage-publisher-exit 30 inspect --format '{{.State.ExitCode}}' "$outage_publisher")
python3 "$helper" shutdown "$run_dir" publisher "$outage_exit_file" publisher-stop-during-outage kafka_outage
run_docker kafka-start 210 compose up -d --no-deps --wait --wait-timeout 180 kafka >/dev/null
python3 "$helper" outage "$run_dir" "$kafka_stopped" "$hold"
run_docker publisher-start 90 compose up -d --no-deps --wait --wait-timeout 60 publisher >/dev/null
# Persistent Kafka fetch failures deliberately stop Activity with safe offsets;
# explicitly perform the documented operator restart after broker recovery.
run_docker activity-recover-after-kafka 90 compose up -d --no-deps --wait --wait-timeout 60 activity >/dev/null
python3 "$helper" recovery "$run_dir" activity kafka_outage activity-recover-after-kafka
phase verify >/dev/null
activity_stopped=$(date +%s)
run_docker activity-stop 30 compose stop -t 12 activity >/dev/null
activity_queue=$(phase enqueue "$run_id-activity" 20)
python3 "$helper" projection-lag "$run_dir" "$activity_queue" 20
sleep "$hold"
phase status >/dev/null
run_docker activity-start 90 compose up -d --no-deps --wait --wait-timeout 60 activity >/dev/null
python3 "$helper" outage "$run_dir" "$activity_stopped" "$hold" activity
phase verify >/dev/null
phase replay >/dev/null
phase limiter "$run_id" 80 >/dev/null
run_docker redis-stop 30 compose stop -t 12 redis >/dev/null
phase redis-outage >/dev/null
run_docker redis-start 90 compose up -d --no-deps --wait --wait-timeout 60 redis >/dev/null
for service in ledger gateway gateway2 activity publisher; do
    run_docker "sigterm-$service" 30 compose stop -t 12 "$service" >/dev/null
    id_file=$(run_docker "container-$service" 30 compose ps -a -q "$service")
    container=$(cat "$id_file")
    exit_file=$(run_docker "exit-$service" 30 inspect --format '{{.State.ExitCode}}' "$container")
    python3 "$helper" shutdown "$run_dir" "$service" "$exit_file"
done
run_docker restore 210 compose up -d --wait --wait-timeout 180 >/dev/null
restored=1
phase verify >/dev/null
