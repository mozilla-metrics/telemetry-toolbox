#!/bin/bash

DAYS_BACK=30
OUTPUT_BASE=$(dirname $0)
if [ ! -z "$1" ]; then
  DAYS_BACK=$1
fi
if [ ! -z "$2" ]; then
  OUTPUT_BASE=$2
fi

function check {
  if [ ! -e "$1" ]; then
    echo "MISSING: $1"
  elif [ ! -s "$1" ]; then
    echo "EMPTY: $1"
#  else
#    echo "OK: $1"
  fi
}

for d in $(seq 1 $DAYS_BACK); do
  day=$(date -d "$d days ago" +%Y%m%d)
#  echo "Checking '$OUTPUT_BASE' $d days ago ($day):"
  check "$OUTPUT_BASE/telemetry/chromehangs/chrome-hangs-$day.txt.gz"
  check "$OUTPUT_BASE/telemetry/flash-versions/telemetry-flash-versions-$day-$day.csv"
  check "$OUTPUT_BASE/telemetry/mobile-flash-versions/telemetry-mobile-flash-versions-$day-$day.csv"
  check "$OUTPUT_BASE/telemetry/shutdown-times/shutdown-times-$day-$day.csv.tar.gz"
  check "$OUTPUT_BASE/telemetry/slowsql/slowsql-main-$day-$day.txt"
  check "$OUTPUT_BASE/telemetry/slowsql/slowsql-other-$day-$day.txt"
done
