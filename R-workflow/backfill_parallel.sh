#!/bin/bash
# Parallel backfill daily reports from 1990-10-01 to today
# Usage: bash backfill_parallel.sh [PARALLEL_JOBS]
# Default parallelism: 4

PARALLEL=${1:-4}
START_DATE="1990-10-01"
END_DATE=$(date +%Y-%m-%d)
WORKDIR=$(pwd)

# Convert to seconds since epoch for iteration
current=$(date -j -f "%Y-%m-%d" "$END_DATE" +%s 2>/dev/null || date -d "$END_DATE" +%s)
end=$(date -j -f "%Y-%m-%d" "$START_DATE" +%s 2>/dev/null || date -d "$START_DATE" +%s)

echo "=== Parallel Backfill Daily Reports ==="
echo "From: $END_DATE → To: $START_DATE"
echo "Parallelism: $PARALLEL"
echo ""
echo "Building pending date list (skipping existing CSVs)..."

# Build list of dates that still need processing
PENDING=()
while [ $current -ge $end ]; do
    date_str=$(date -j -f "%s" "$current" +%Y-%m-%d 2>/dev/null || date -d "@$current" +%Y-%m-%d)
    date_compact=$(echo "$date_str" | tr -d '-')
    if [ ! -f "$WORKDIR/hydroshare/droughtData${date_compact}.csv" ]; then
        PENDING+=("$date_str")
    fi
    current=$((current - 86400))
done

total=${#PENDING[@]}
echo "Dates to process: $total"
echo ""

if [ $total -eq 0 ]; then
    echo "Nothing to do — all CSVs already exist."
    exit 0
fi

# Counters (written to temp files so subshells can update them)
TMPDIR_COUNTERS=$(mktemp -d)
echo 0 > "$TMPDIR_COUNTERS/done"
echo 0 > "$TMPDIR_COUNTERS/failed"

run_date() {
    local date_str="$1"
    docker run --rm \
        --env-file "$WORKDIR/.env" \
        -v "$WORKDIR/hydroshare:/app/hydroshare" \
        rezviz "$date_str" > /dev/null 2>&1

    if [ $? -eq 0 ]; then
        echo "  ✓ $date_str"
    else
        echo "  ✗ FAILED: $date_str"
    fi
}

export -f run_date
export WORKDIR

queued=0
for date_str in "${PENDING[@]}"; do
    queued=$((queued + 1))

    # Launch in background
    run_date "$date_str" &

    # Throttle: wait until active jobs drop below PARALLEL
    while [ "$(jobs -rp | wc -l | tr -d ' ')" -ge "$PARALLEL" ]; do
        sleep 0.5
    done

    # Progress every 10 queued
    if [ $((queued % 10)) -eq 0 ]; then
        echo "[queued $queued/$total | active: $(jobs -rp | wc -l | tr -d ' ')]"
    fi
done

echo ""
echo "Waiting for final jobs to finish..."
wait

rm -rf "$TMPDIR_COUNTERS"
echo ""
echo "=== Backfill Complete ==="
