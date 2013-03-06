#!/bin/bash

. /home/metrics-etl/prod/daily-telemetry-setvars.sh

cd $ETL_HOME

# Telemetry Chrome Hangs
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_chrome_hangs.pig
hadoop fs -getmerge chrome-hangs-$YESTERDAY-$YESTERDAY $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt
/usr/bin/python $ETL_HOME/symbolicate.py -i $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt -o $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.modified
mv $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.modified $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz
size=`stat --printf="%s" $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz`
if [ $size -gt 0 ]
then
    chmod 644 $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz
    scp $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt.gz $CHROMEHANG_DEST
fi
rm $ETL_HOME/telemetry/chromehangs/chrome-hangs-$YESTERDAY.txt
