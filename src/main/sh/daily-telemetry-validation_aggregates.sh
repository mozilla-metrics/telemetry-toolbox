#!/bin/bash

. /home/metrics-etl/prod/daily-telemetry-setvars.sh

cd $ETL_HOME

# Telemetry Validation
${PIG_HOME}/bin/pig -f $ETL_HOME/validate_telemetry_submissions.pig -p start_date=$YESTERDAY -p end_date=$YESTERDAY -p input_table=telemetry -p output_table=telemetry

# Telemetry Aggregates (should be run after Validation)
#${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY -param index_name=$INDEX_NAME -param es_config_path=$ES_CONFIG -param es_plugins_path=$ES_PLUGINS $ETL_HOME/telemetry_aggregates_old.pig
${PIG_HOME}/bin/pig -param start_date=$YESTERDAY -param end_date=$YESTERDAY -param index_name=$VALID_INDEX_NAME -param es_config_path=$ES_CONFIG -param es_plugins_path=$ES_PLUGINS $ETL_HOME/telemetry_aggregates.pig
