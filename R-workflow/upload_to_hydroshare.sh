#!/bin/bash
# Upload droughtData CSVs from 1990-10-01 to 2026-02-09 to HydroShare

RESOURCE_ID="22b2f10103e5426a837defc00927afbd"
HYDROSHARE_BASE="https://www.hydroshare.org"
HYDROSHARE_DIR="$(pwd)/hydroshare"
LOG_FILE="$(pwd)/upload_dataurls.log"

# Load credentials from .env
export $(grep -v '^#' "$(pwd)/.env" | xargs)

if [ -z "$HYDROSHARE_USERNAME" ] || [ -z "$HYDROSHARE_PASSWORD" ]; then
  echo "ERROR: HYDROSHARE_USERNAME or HYDROSHARE_PASSWORD not set in .env"
  exit 1
fi

START_DATE="1994-11-04"
END_DATE="2026-02-09"

current=$(date -j -f "%Y-%m-%d" "$START_DATE" +%s 2>/dev/null || date -d "$START_DATE" +%s)
end=$(date -j -f "%Y-%m-%d" "$END_DATE" +%s 2>/dev/null || date -d "$END_DATE" +%s)

total=$(( (end - current) / 86400 + 1 ))
count=0
skipped=0
failed=0

echo "=== HydroShare Upload ===" | tee "$LOG_FILE"
echo "Date range: $START_DATE to $END_DATE ($total days)" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

while [ $current -le $end ]; do
  date_str=$(date -j -f "%s" "$current" +%Y-%m-%d 2>/dev/null || date -d "@$current" +%Y-%m-%d)
  date_compact=$(echo "$date_str" | tr -d '-')
  filename="droughtData${date_compact}.csv"
  filepath="$HYDROSHARE_DIR/$filename"
  count=$((count + 1))

  if [ ! -f "$filepath" ]; then
    echo "[$count/$total] SKIP (no file): $filename" | tee -a "$LOG_FILE"
    skipped=$((skipped + 1))
    current=$((current + 86400))
    continue
  fi

  # Upload file with retry on connection failure (000)
  max_retries=5
  attempt=0
  http_status="000"

  while [[ "$http_status" == "000" && $attempt -lt $max_retries ]]; do
    if [[ $attempt -gt 0 ]]; then
      echo "  Retrying in 30s (attempt $attempt/$max_retries)..." | tee -a "$LOG_FILE"
      sleep 30
    fi
    http_status=$(curl -s -o /dev/null -w "%{http_code}" --max-time 60 \
      -u "${HYDROSHARE_USERNAME}:${HYDROSHARE_PASSWORD}" \
      -F "file=@${filepath}" \
      "${HYDROSHARE_BASE}/hsapi/resource/${RESOURCE_ID}/files/")
    attempt=$((attempt + 1))
  done

  if [[ "$http_status" == "201" ]]; then
    echo "[$count/$total] OK (201): $filename" | tee -a "$LOG_FILE"
  elif [[ "$http_status" == "400" ]]; then
    echo "[$count/$total] ALREADY EXISTS (400): $filename" | tee -a "$LOG_FILE"
  else
    echo "[$count/$total] FAILED ($http_status): $filename" | tee -a "$LOG_FILE"
    failed=$((failed + 1))
  fi

  sleep 2

  current=$((current + 86400))
done

echo "" | tee -a "$LOG_FILE"
echo "=== Done ===" | tee -a "$LOG_FILE"
echo "Uploaded: $((count - skipped - failed)) | Skipped: $skipped | Failed: $failed" | tee -a "$LOG_FILE"
