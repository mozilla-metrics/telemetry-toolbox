#!/bin/bash
. $(dirname $0)/daily-telemetry-setvars.sh
cd $ETL_HOME
LOG_FINAL=$ETL_HOME/logs/daily-telemetry-shutdown-times.log
LOG=$LOG_FINAL.$YESTERDAY

python $ETL_HOME/simple_query.py \
   -n shutdown_times_daily.pig \
   -s $YESTERDAY -e $YESTERDAY \
   -f simpleMeasurements.shutdownDuration,info.OS,info.appVersion,info.appBuildID \
   -o shutdown_times_$YESTERDAY \
   -a 0.25 \
   "shutdownDuration != null" > $LOG 2>&1

SHUTDOWN_RESULT=$?
if [ "$SHUTDOWN_RESULT" -eq "0" ]; then
   echo "Telemetry Shutdown Times succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Shutdown Times failed (code $SHUTDOWN_RESULT) for $YESTERDAY. Check $LOG for details."
   exit 2
fi

cd telemetry/shutdown-times
hadoop dfs -getmerge /user/metrics-etl/shutdown_times_$YESTERDAY $ETL_HOME/telemetry/shutdown-times/shutdown-times-$YESTERDAY-$YESTERDAY.csv >> $LOG 2>&1
if [ "$?" -ne "0" ]; then
   echo "ERROR: Failed to get Shutdown Times from HDFS. Check $LOG for details."
   exit 3
fi

tar cvzf shutdown-times-$YESTERDAY-$YESTERDAY.csv.tar.gz shutdown-times-$YESTERDAY-$YESTERDAY.csv >> $LOG 2>&1
if [ "$?" -ne "0" ]; then
   echo "ERROR: Failed to compress Shutdown Times. Check $LOG for details."
   exit 4
fi
scp $ETL_HOME/telemetry/shutdown-times/shutdown-times-$YESTERDAY-$YESTERDAY.csv.tar.gz $SHUTDOWN_TIMES_DEST/ >> $LOG 2>&1
if [ "$?" -ne "0" ]; then
   echo "ERROR: Failed to export Shutdown Times to $SHUTDOWN_TIMES_DEST.  Check $LOG for details."
   exit 5
fi

mv $LOG $LOG_FINAL
