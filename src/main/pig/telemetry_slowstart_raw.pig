register 'akela-0.3-SNAPSHOT.jar'

SET pig.logfile telemetry-slowstart-raw.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' AND
                            json_map#'simpleMeasurements'#'uptime' < 5 AND
                            json_map#'simpleMeasurements'#'firstPaint' > 30000 AND
                            json_map#'simpleMeasurements'#'startupInterrupted' == 0 AND
                            json_map#'info'#'OS' == 'WINNT' AND
                            json_map#'info'#'appUpdateChannel' IS NOT NULL;
json_only = FOREACH filtered GENERATE json;
STORE json_only INTO 'telemetry-slowstart-$start_date-$end_date';