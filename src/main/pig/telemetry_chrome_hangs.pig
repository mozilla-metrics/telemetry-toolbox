/* Aggregate all telemetry data for a given day and index the aggregate json objects in ElasticSearch */
register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry-chrome-hangs.log;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'chromeHangs' IS NOT NULL AND 
                                   com.mozilla.pig.eval.Size(json_map#'chromeHangs') > 0 AND
                                   json_map#'info'#'appUpdateChannel' == 'nightly' AND
                                   json_map#'info'#'OS' == 'WINNT';
orig_data = FOREACH filtered_genmap GENERATE k,json;
STORE orig_data INTO 'chrome-hangs-$start_date-$end_date';