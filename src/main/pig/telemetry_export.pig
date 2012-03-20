register 'akela-0.3-SNAPSHOT.jar'

SET pig.logfile telemetry-export.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
/*
SET output.compression.enabled true;
SET output.compression.codec com.hadoop.compression.lzo.LzopCodec;
*/

/*raw = LOAD 'hbase://telemetry' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);*/
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];

/* modify based on your needs and what you're after */
filtered = FILTER genmap BY json_map#'info'#'appName' == 'Firefox';
limited = LIMIT filtered 100;

origdata = FOREACH limited GENERATE k,json;

STORE origdata INTO 'telemetry-export' USING PigStorage();
