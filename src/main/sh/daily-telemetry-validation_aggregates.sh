#!/bin/bash

. $(dirname $0)/daily-telemetry-setvars.sh

cd $ETL_HOME
LOG_FINAL=$ETL_HOME/logs/daily-telemetry-validation_aggregates.log
LOG=$LOG_FINAL.$YESTERDAY

# Telemetry Validation
${PIG_HOME}/bin/pig -f $ETL_HOME/validate_telemetry_submissions.pig -p start_date=$YESTERDAY -p end_date=$YESTERDAY -p input_table=telemetry -p output_table=telemetry > $LOG 2>&1
VALIDATE_RESULT=$?
### NOTE: pig exit codes (per http://ofps.oreilly.com/titles/9781449302641/running_pig.html)
# 0  Success
# 1  Retriable failure
# 2  Failure
# 3  Partial failure
# 4  Illegal Arguments
# 5  IOException thrown
# 6  PigException thrown
# 7  ParseException thrown
# 8  Other Throwable thrown
if [ "$VALIDATE_RESULT" -eq "0" ]; then
   echo "Telemetry Validation succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Validation failed (code $VALIDATE_RESULT) for $YESTERDAY.  Check $LOG for more details."
   exit 2
fi

# Telemetry Aggregates (should be run after Validation)
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY -param index_name=$VALID_INDEX_NAME -param es_config_path=$ES_CONFIG -param es_plugins_path=$ES_PLUGINS $ETL_HOME/telemetry_aggregates.pig >> $LOG 2>&1
AGGREGATE_RESULT=$?
if [ "$AGGREGATE_RESULT" -eq "0" ]; then
   echo "Telemetry Aggregation succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Aggregation failed (code $AGGREGATE_RESULT) for $YESTERDAY.  Check $LOG for more details."
   exit 3
fi

mv $LOG $LOG_FINAL
