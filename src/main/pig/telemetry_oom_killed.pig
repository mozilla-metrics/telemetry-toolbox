register 'akela-0.5-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'

SET pig.logfile telemetry_oom_killed.log;
SET default_parallel 53;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define HistogramTuples com.mozilla.telemetry.pig.eval.HistogramTuples();
define HistogramValueTuples com.mozilla.telemetry.pig.eval.HistogramValueTuples();
define ConvertNull com.mozilla.pig.eval.ConvertNull('NA');
define IsMap com.mozilla.pig.filter.map.IsMap();
define KernelToAndroidVersion com.mozilla.telemetry.pig.eval.KernelToAndroidVersion();
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9](\\.*[0-9]*){1,2}');

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:bytearray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY IsMap(json_map#'info') AND 
                                   IsMap(json_map#'histograms') AND
                                   IsMap(json_map#'simpleMeasurements') AND
                                   json_map#'info'#'appName' == 'Fennec' AND
                                   (json_map#'info'#'reason' == 'idle-daily' OR json_map#'info'#'reason' == 'saved-session');
                                   
hist_values = FOREACH filtered_genmap GENERATE SUBSTRING((chararray)k,1,9) AS d:chararray, 
                                               (chararray)json_map#'info'#'reason' AS reason:chararray,
                                               (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                                               ConvertNull((chararray)json_map#'info'#'appUpdateChannel') AS product_channel:chararray, 
                                               (chararray)json_map#'info'#'arch' AS arch:chararray,
                                               ((chararray)json_map#'info'#'hasARMv6' == 'true' ? 1 : 0) AS has_armv6:int,
                                               ((chararray)json_map#'info'#'hasARMv7' == 'true' ? 1 : 0) AS has_armv7:int,
                                               (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               KernelToAndroidVersion((chararray)json_map#'info'#'version') AS os_version:chararray,
                                               OsVersionNormalizer((chararray)json_map#'info'#'version') AS normalized_kernel_version:chararray,
                                               (chararray)json_map#'info'#'version' AS kernel_version:chararray,
                                               (json_map#'histograms'#'OUT_OF_MEMORY_KILLED' IS NOT NULL ? 1 : 0) AS has_oom_killed:int;

/*
grpd_all = GROUP hist_values ALL;
ndocs = FOREACH grpd_all GENERATE COUNT(hist_values) AS doc_count:long;
*/

grpd_hist_values = GROUP hist_values BY (d,reason,product_version,product_channel,arch,has_armv6,has_armv7,os,os_version,normalized_kernel_version,kernel_version,has_oom_killed);
counts = FOREACH grpd_hist_values GENERATE FLATTEN(group) AS (d,reason,product_version,product_channel,arch,has_armv6,has_armv7,os,os_version,normalized_kernel_version,kernel_version,has_oom_killed),
                                           COUNT(hist_values) AS cnt:long;

ordered_by_count = ORDER counts BY cnt DESC;
STORE ordered_by_count INTO 'telemetry-oomkilled-counts-$start_date-$end_date' USING PigStorage(',');