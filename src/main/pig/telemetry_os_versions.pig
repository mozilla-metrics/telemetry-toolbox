/* Aggregate all telemetry data in preparation for indexing the results using AggregateElasticSearchIndexer */
register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry-os-versions.log;
SET default_parallel 16;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9](\\.*[0-9]*){1}');

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY (json_map#'info'#'appName' == 'Firefox' OR 
                                   json_map#'info'#'appName' == 'Thunderbird' OR 
                                   json_map#'info'#'appName' == 'Fennec') AND
                                   (json_map#'info'#'reason' == 'idle-daily' OR json_map#'info'#'reason' == 'saved-session');
/* Create a dataset for generating histogram name level counts */
os_versions = FOREACH filtered_genmap GENERATE (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               (chararray)json_map#'info'#'version' AS os_version:chararray,
                                               OsVersionNormalizer((chararray)json_map#'info'#'version') AS norm_os_version:chararray;
grpd = GROUP os_versions BY (os,os_version);
counts = FOREACH grpd GENERATE FLATTEN(group) AS (os,os_version), COUNT(os_versions) AS count:long;
ordered = ORDER counts BY os,os_version,count DESC;

STORE ordered INTO 'os-version-counts';

norm_grpd = GROUP os_versions BY (os,norm_os_version);
norm_counts = FOREACH norm_grpd GENERATE FLATTEN(group) AS (os,norm_os_version), COUNT(os_versions) AS count:long;
norm_ordered = ORDER norm_counts BY os,norm_os_version,count DESC;

STORE norm_ordered INTO 'norm-os-version-counts';