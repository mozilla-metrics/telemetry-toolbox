register 'akela-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry-export.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;


/*raw = LOAD 'hbase://telemetry' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);*/
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111101', '20111130', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered = FILTER genmap BY json_map#'info'#'appName' == 'Firefox';
gendata = FOREACH filtered GENERATE json_map#'info'#'appVersion' AS version:chararray, json_map#'simpleMeasurements'#'sessionRestored' AS session_restored:long;
STORE gendata INTO 'telemetry-sessionrestored' USING PigStorage(',');

SET output.compression.enabled true;
SET output.compression.codec com.hadoop.compression.lzo.LzopCodec;