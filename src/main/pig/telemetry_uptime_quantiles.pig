register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'datafu-0.0.4.jar'

SET pig.logfile telemetry-aggregates.log;
SET default_parallel 16;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define ConvertNull com.mozilla.pig.eval.ConvertNull('NA');
define SimpleMeasureTuples com.mozilla.telemetry.pig.eval.SimpleMeasureTuples();
define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75', '0.95', '0.99', '1.0');

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY (json_map#'info'#'appName' == 'Firefox' OR 
                                   json_map#'info'#'appName' == 'Thunderbird' OR 
                                   json_map#'info'#'appName' == 'Fennec') AND
                                   (json_map#'info'#'reason' == 'idle-daily' OR json_map#'info'#'reason' == 'saved-session');
uptimes = FOREACH filtered_genmap GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                                           (chararray)json_map#'info'#'appName' AS product:chararray,
                                           (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                                           (int)json_map#'simpleMeasurements'#'uptime' AS uptime:int;
filtered_nulls = FILTER uptimes BY uptime IS NOT NULL AND uptime > 0 AND uptime < 1441;
grpd = GROUP filtered_nulls BY (d,product,product_version);
quants_by_factors = FOREACH grpd GENERATE group, Quantile(filtered_nulls.uptime);

STORE quants_by_factors INTO 'uptime-quantiles-$start_date-$end_date';