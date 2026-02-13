#!/bin/bash
# Backfill daily reports from today back to 1990-10-01
# Uses Docker image to generate each day's report

START_DATE="1990-10-01"
END_DATE=$(date +%Y-%m-%d)

# Convert to seconds since epoch for iteration
current=$(date -j -f "%Y-%m-%d" "$END_DATE" +%s 2>/dev/null || date -d "$END_DATE" +%s)
end=$(date -j -f "%Y-%m-%d" "$START_DATE" +%s 2>/dev/null || date -d "$START_DATE" +%s)

count=0
total=$(( (current - end) / 86400 + 1 ))

echo "=== Backfill Daily Reports ==="
echo "From: $END_DATE"
echo "To: $START_DATE"
echo "Total days: $total"
echo ""

while [ $current -ge $end ]; do
    # Format date
    date_str=$(date -j -f "%s" "$current" +%Y-%m-%d 2>/dev/null || date -d "@$current" +%Y-%m-%d)
    count=$((count + 1))
    
    echo "[$count/$total] Processing $date_str..."
    
    # Run docker with the date
    docker run --rm --env-file .env -v $(pwd)/hydroshare:/app/hydroshare rezviz "$date_str" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        echo "  Done"
    else
        echo "  FAILED"
    fi
    
    # Move to previous day
    current=$((current - 86400))
done

echo ""
echo "=== Backfill Complete ==="
echo "Generated $count daily reports"
