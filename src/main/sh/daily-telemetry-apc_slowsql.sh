#!/bin/bash

. /home/metrics-etl/prod/daily-telemetry-setvars.sh

cd $ETL_HOME

# Telemetry Addon Privacy Correction (must run before slow sql)
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_apc.pig

# Telemetry Slow SQL
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY $ETL_HOME/telemetry_slowsql.pig
hadoop fs -getmerge slowsql-main-$YESTERDAY-$YESTERDAY /tmp/slowsql-main-$YESTERDAY-$YESTERDAY.txt
cat $ETL_HOME/telemetry/slowsql/header.txt > $ETL_HOME/telemetry/slowsql/slowsql-main-$YESTERDAY-$YESTERDAY.txt
cat /tmp/slowsql-main-$YESTERDAY-$YESTERDAY.txt >> $ETL_HOME/telemetry/slowsql/slowsql-main-$YESTERDAY-$YESTERDAY.txt

hadoop fs -getmerge slowsql-other-$YESTERDAY-$YESTERDAY /tmp/slowsql-other-$YESTERDAY-$YESTERDAY.txt
cat $ETL_HOME/telemetry/slowsql/header.txt > $ETL_HOME/telemetry/slowsql/slowsql-other-$YESTERDAY-$YESTERDAY.txt
cat /tmp/slowsql-other-$YESTERDAY-$YESTERDAY.txt >> $ETL_HOME/telemetry/slowsql/slowsql-other-$YESTERDAY-$YESTERDAY.txt
