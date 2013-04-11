#!/bin/bash

. $(dirname $0)/daily-telemetry-setvars.sh

cd $ETL_HOME
LOG_FINAL=$ETL_HOME/logs/daily-telemetry-apc_slowsql.log
LOG=$LOG_FINAL.$YESTERDAY

# Telemetry Addon Privacy Correction (must run before slow sql)
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_apc.pig > $LOG 2>&1
APC_RESULT=$?
#### See daily-telemetry-validation_aggregates.sh for exit code descriptions.
if [ "$APC_RESULT" -eq "0" ]; then
   echo "Telemetry Addon Privacy Correction succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Addon Privacy Correction failed (code $APC_RESULT) for $YESTERDAY.  Check $LOG for more details."
   echo "ERROR: APC Failure means Slow SQL Export did not run either."
   exit 2
fi

# Telemetry Slow SQL
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_slowsql.pig >> $LOG 2>&1
SLOWSQL_RESULT=$?
if [ "$SLOWSQL_RESULT" -eq "0" ]; then
   echo "Telemetry Slow SQL succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Slow SQL failed (code $SLOWSQL_RESULT) for $YESTERDAY.  Check $LOG for more details."
   exit 3
fi

hadoop fs -getmerge slowsql-main-$YESTERDAY-$YESTERDAY $ETL_HOME/telemetry/slowsql/slowsql-main-$YESTERDAY-$YESTERDAY.txt
hadoop fs -getmerge slowsql-other-$YESTERDAY-$YESTERDAY $ETL_HOME/telemetry/slowsql/slowsql-other-$YESTERDAY-$YESTERDAY.txt

mv $LOG $LOG_FINAL
