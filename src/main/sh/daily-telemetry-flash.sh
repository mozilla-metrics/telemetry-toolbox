#!/bin/bash

. /home/metrics-etl/prod/daily-telemetry-setvars.sh

cd $ETL_HOME
LOG=$ETL_HOME/logs/daily-telemetry-flash.log

# Telemetry Flash Versions
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_flash_versions.pig > $LOG 2>&1
FLASH_RESULT=$?
#### See daily-telemetry-validation_aggregates.sh for exit code descriptions.
if [ "$FLASH_RESULT" -eq "0" ]; then
   echo "Telemetry Flash Versions succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Flash Versions failed (code $FLASH_RESULT) for $YESTERDAY.  Check $LOG for more details."
   echo "ERROR: Flash Failure means Mobile Flash Export did not run either."
   exit 2
fi

hadoop fs -getmerge telemetry-flash-versions-$YESTERDAY-$YESTERDAY /tmp/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat $ETL_HOME/telemetry/flash-versions/header.txt > $ETL_HOME/telemetry/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat /tmp/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv >> $ETL_HOME/telemetry/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
scp $ETL_HOME/telemetry/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv ${FLASH_VERSIONS_DEST}/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv
if [ "$?" -eq "0" ]; then
   echo "Flash Versions exported successfully"
else
   echo "ERROR: Failed to export Flash Versions"
fi
ssh $PUB_SERVER "chmod 644 $FLASH_VERSIONS_PATH/flash-versions/telemetry-flash-versions-$YESTERDAY-$YESTERDAY.csv"

# Telemetry Mobile Flash Versions
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_mobile_flash_versions.pig >> $LOG 2>&1
FLASHMOB_RESULT=$?
if [ "$FLASHMOB_RESULT" -eq "0" ]; then
   echo "Telemetry Mobile Flash Versions succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Mobile Flash Versions failed (code $FLASHMOB_RESULT) for $YESTERDAY.  Check $LOG for more details."
   exit 3
fi

hadoop fs -getmerge telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY /tmp/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat $ETL_HOME/telemetry/mobile-flash-versions/header.txt > $ETL_HOME/telemetry/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
cat /tmp/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv >> $ETL_HOME/telemetry/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
scp $ETL_HOME/telemetry/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv ${FLASH_VERSIONS_DEST}/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv
if [ "$?" -eq "0" ]; then
   echo "Mobile Flash Versions exported successfully"
else
   echo "ERROR: Failed to export Mobile Flash Versions"
fi
ssh $PUB_SERVER "chmod 644 $FLASH_VERSIONS_PATH/mobile-flash-versions/telemetry-mobile-flash-versions-$YESTERDAY-$YESTERDAY.csv"
