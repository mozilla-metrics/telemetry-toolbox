#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-sun
PIG_HOME=/usr/lib/pig
ETL_HOME=/home/etl
ES_CONFIG=$ETL_HOME/elasticsearch/elasticsearch.yml
ES_PLUGINS=$ETL_HOME/elasticsearch/plugins
YESTERDAY="`date +%Y%m%d --date="1 day ago"`"
YESTERDAY_YYYY_MM="`date +%Y%m --date="1 day ago"`"
VALID_INDEX_NAME="telemetry_agg_valid_$YESTERDAY_YYYY_MM"
PUB_SERVER=example.mozilla.com
HDFS_EXPORT_PATH=hdfs://example.mozilla.com:8020/user/$USER
CHROMEHANG_DEST=${PUB_SERVER}:/var/www/public/telemetry/chrome-hangs/
FLASH_VERSIONS_PATH=/var/www/public/telemetry
FLASH_VERSIONS_DEST=${PUB_SERVER}:${FLASH_VERSIONS_PATH}
