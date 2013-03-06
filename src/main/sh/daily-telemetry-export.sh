#!/bin/bash

. /home/metrics-etl/prod/daily-telemetry-setvars.sh

cd $ETL_HOME

# Telemetry Export
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY -param output=${HDFS_EXPORT_PATH}/telemetry-export-$YESTERDAY $ETL_HOME/telemetry_export.pig
