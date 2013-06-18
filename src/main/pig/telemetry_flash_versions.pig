/* Aggregate all telemetry data for a given day and index the aggregate json objects in ElasticSearch */
register 'akela-0.5-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'

SET pig.logfile telemetry_flash_versions.log;
SET default_parallel 53;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compress false;

define ConvertNull com.mozilla.pig.eval.ConvertNull('NA');
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9](\\.*[0-9]*){1}');
define IsMap com.mozilla.pig.filter.map.IsMap();

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:bytearray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY IsMap(json_map#'info') AND 
                                   IsMap(json_map#'histograms') AND
                                   IsMap(json_map#'simpleMeasurements') AND
                                   (json_map#'info'#'appName' == 'Firefox' OR json_map#'info'#'appName' == 'MetroFirefox') AND
                                   (json_map#'info'#'reason' == 'idle-daily' OR json_map#'info'#'reason' == 'saved-session') AND
                                   json_map#'info'#'OS' == 'WINNT';
flash_data = FOREACH filtered_genmap GENERATE (chararray)json_map#'info'#'appName' AS product:chararray,
                                              (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                                              ConvertNull((chararray)json_map#'info'#'appUpdateChannel') AS product_channel:chararray,
                                              (chararray)json_map#'info'#'OS' AS os:chararray, 
                                              OsVersionNormalizer((chararray)json_map#'info'#'version') AS os_version:chararray,
                                              (json_map#'info'#'flashVersion' IS NULL ? 'NA' : (chararray)json_map#'info'#'flashVersion') AS flash_version:chararray;
grpd_flash = GROUP flash_data BY (product,product_version,product_channel,os,os_version,flash_version);
flash_counts = FOREACH grpd_flash GENERATE FLATTEN(group) AS (product,product_version,product_channel,os,os_version,flash_version), 
                                           COUNT(flash_data) AS cnt:long;
order_by_cnt = ORDER flash_counts BY cnt DESC;

STORE order_by_cnt INTO 'telemetry-flash-versions-$start_date-$end_date' USING PigStorage(',');
