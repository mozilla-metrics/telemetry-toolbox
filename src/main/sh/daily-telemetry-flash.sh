#!/bin/bash

. /home/metrics-etl/prod/daily-telemetry-setvars.sh

cd $ETL_HOME

# Telemetry Flash Versions
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_flash_versions.pig
hadoop fs -getmerge telemetry-flash-versions-$YESTERDAY-$YESTERDAY /tmp/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat $ETL_HOME/telemetry/flash-versions/header.txt > $ETL_HOME/telemetry/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat /tmp/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv >> $ETL_HOME/telemetry/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
scp $ETL_HOME/telemetry/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv ${FLASH_VERSIONS_DEST}/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
ssh $PUB_SERVER "chmod 644 $FLASH_VERSIONS_PATH/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv"
# Telemetry Mobile Flash Versions
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_mobile_flash_versions.pig
hadoop fs -getmerge telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY /tmp/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat $ETL_HOME/telemetry/mobile-flash-versions/header.txt > $ETL_HOME/telemetry/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat /tmp/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv >> $ETL_HOME/telemetry/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
scp $ETL_HOME/telemetry/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv ${FLASH_VERSIONS_DEST}/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
ssh $PUB_SERVER "chmod 644 $FLASH_VERSIONS_PATH/mobile-flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv"
