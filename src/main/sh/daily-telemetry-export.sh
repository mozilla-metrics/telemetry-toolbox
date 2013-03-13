#!/bin/bash

. /home/metrics-etl/prod/daily-telemetry-setvars.sh

cd $ETL_HOME
LOG=$ETL_HOME/logs/daily-telemetry-export.log

# Telemetry Export
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY -param output=${HDFS_EXPORT_PATH}/telemetry-export-$YESTERDAY $ETL_HOME/telemetry_export.pig > $LOG 2>&1
EXPORT_RESULT=$?
#### See daily-telemetry-validation_aggregates.sh for exit code descriptions.
if [ "$EXPORT_RESULT" -eq "0" ]; then
   echo "Telemetry Export succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Export failed (code $EXPORT_RESULT) for $YESTERDAY.  Check $LOG for more details."
   exit 2
fi
