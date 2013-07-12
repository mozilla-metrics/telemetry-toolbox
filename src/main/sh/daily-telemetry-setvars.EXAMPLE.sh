#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-sun
export HADOOP_CONF_DIR=/etc/hadoop/conf
export PIG_CLASSPATH=$HADOOP_CONF_DIR

PIG_HOME=/usr/lib/pig
ETL_HOME=/home/etl
ES_CONFIG=$ETL_HOME/elasticsearch/elasticsearch.yml
ES_PLUGINS=$ETL_HOME/elasticsearch/plugins

## Default to running for yesterday.
YESTERDAY="`date +%Y%m%d --date="1 day ago"`"
YESTERDAY_YYYY_MM="`date +%Y%m --date="1 day ago"`"

## If a parameter was passed in, use that for the target day:
if [ ! -z "$1" ]; then
  YESTERDAY=$(date -d "$1" +%Y%m%d)
  YESTERDAY_YYYY_MM=$(date -d "$1" +%Y%m)
fi

VALID_INDEX_NAME="telemetry_agg_valid_$YESTERDAY_YYYY_MM"
PUB_SERVER=example.mozilla.com
HDFS_EXPORT_PATH=hdfs://example.mozilla.com:8020/user/$USER
CHROMEHANG_DEST=${PUB_SERVER}:/var/www/public/telemetry/chrome-hangs/
SHUTDOWN_TIMES_DEST=${PUB_SERVER}:/var/www/public/telemetry/shutdown-times/
FLASH_VERSIONS_PATH=/var/www/public/telemetry
FLASH_VERSIONS_DEST=${PUB_SERVER}:${FLASH_VERSIONS_PATH}
