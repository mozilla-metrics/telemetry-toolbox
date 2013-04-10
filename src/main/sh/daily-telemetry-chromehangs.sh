#!/bin/bash

. $(dirname $0)/daily-telemetry-setvars.sh

cd $ETL_HOME
LOG_FINAL=$ETL_HOME/logs/daily-telemetry-chromehangs.log
LOG=$LOG_FINAL.$YESTERDAY

# Telemetry Chrome Hangs
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_chrome_hangs.pig > $LOG 2>&1
CHROME_RESULT=$?
#### See daily-telemetry-validation_aggregates.sh for exit code descriptions.
if [ "$CHROME_RESULT" -eq "0" ]; then
   echo "Telemetry Chrome Hangs succeeded for $YESTERDAY"
else
   echo "ERROR: Telemetry Chrome Hangs failed (code $CHROME_RESULT) for $YESTERDAY.  Check $LOG for more details."
   exit 2
fi

hadoop fs -getmerge chrome-hangs-$YESTERDAY-$YESTERDAY $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt
/usr/bin/python $ETL_HOME/symbolicate.py -i $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt -o $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.modified >> $LOG 2>&1

SYMBOL_RESULT=$?
if [ "$SYMBOL_RESULT" -eq "0" ]; then
   echo "Symbolication succeeded for $YESTERDAY"
else
   echo "ERROR: Symbolication failed (code $SYMBOL_RESULT) for $YESTERDAY.  Check $LOG for more details."
   exit 3
fi

mv $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.modified $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz
size=`stat --printf="%s" $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz`
if [ $size -gt 0 ]; then
   chmod 644 $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz
   scp $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz $CHROMEHANG_DEST
   if [ "$?" -eq "0" ]; then
      echo "Successfully exported Chrome Hangs"
   else
      echo "ERROR: Failed to export Chrome Hangs"
      exit 4
   fi
else
   echo "ERROR: Symbolicated Chrome Hangs file was empty."
   exit 5
fi
rm $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt

mv $LOG $LOG_FINAL
