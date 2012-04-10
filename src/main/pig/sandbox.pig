register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry-sandbox.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
/*SET pig.cachedbag.memusage 0.05;*/

define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75', '0.95', '0.99', '1.0');

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20120402', '20120127', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'simpleMeasurements'#'startupCrashDetectionEnd' IS NOT NULL;
scdes = FOREACH genmap GENERATE json_map#'simpleMeasurements'#'startupCrashDetectionEnd' AS scde:long;
grpd = GROUP scdes ALL;
quants = FOREACH grpd GENERATE Quantile(scde);

STORE quants INTO 'startupCrashDetectionEnd-quantiles';