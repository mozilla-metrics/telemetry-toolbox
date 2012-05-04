/* Aggregate all telemetry data in preparation for indexing the results using AggregateElasticSearchIndexer */
register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'wonderdog-1.0-SNAPSHOT.jar'
register './elasticsearch/lib/*.jar'

SET pig.logfile telemetry-aggregates.log;
SET default_parallel 16;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define HistogramValueTuples com.mozilla.telemetry.pig.eval.HistogramValueTuples();
define HistogramNames com.mozilla.telemetry.pig.eval.HistogramNames();
define ConvertNull com.mozilla.pig.eval.ConvertNull('NA');
define OsVersionNormalizer com.mozilla.pig.eval.regex.FindOrReturn('^[0-9](\\.*[0-9]*){1}');
define IsMap com.mozilla.pig.filter.map.IsMap();
define AggregateJson com.mozilla.telemetry.pig.eval.json.AggregateJson();

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY IsMap(json_map#'info') AND 
                                   IsMap(json_map#'histograms') AND
                                   IsMap(json_map#'simpleMeasurements') AND
                                   (json_map#'info'#'appName' == 'Firefox' OR 
                                    json_map#'info'#'appName' == 'Thunderbird' OR 
                                    json_map#'info'#'appName' == 'Fennec') AND
                                   (json_map#'info'#'reason' == 'idle-daily' OR json_map#'info'#'reason' == 'saved-session');
/* Create a dataset for generating histogram name level counts */
hist_names = FOREACH filtered_genmap GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                                              (chararray)json_map#'info'#'appName' AS product:chararray,
                                              (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                                              ConvertNull((chararray)json_map#'info'#'appUpdateChannel') AS product_channel:chararray,
                                              (chararray)json_map#'info'#'arch' AS arch:chararray,
                                              (chararray)json_map#'info'#'OS' AS os:chararray, 
                                              OsVersionNormalizer((chararray)json_map#'info'#'version') AS os_version:chararray,
                                              (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                              (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                              FLATTEN(HistogramNames(json_map#'histograms', json_map#'simpleMeasurements')) AS hist_name:chararray;
hist_by_name = GROUP hist_names BY (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name);
hist_name_counts = FOREACH hist_by_name GENERATE FLATTEN(group), COUNT(hist_names) AS doc_count:long;

/* Create a dataset for generating histogram name and value counts */
hist_values = FOREACH filtered_genmap GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                                               (chararray)json_map#'info'#'appName' AS product:chararray,
                                               (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                                               ConvertNull((chararray)json_map#'info'#'appUpdateChannel') AS product_channel:chararray, 
                                               (chararray)json_map#'info'#'arch' AS arch:chararray,
                                               (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               OsVersionNormalizer((chararray)json_map#'info'#'version') AS os_version:chararray,
                                               (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                               (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                               FLATTEN(HistogramValueTuples(json_map#'histograms', json_map#'simpleMeasurements')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
hist_by_name_and_v = GROUP hist_values BY (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name,v);
hist_sums = FOREACH hist_by_name_and_v GENERATE FLATTEN(group),
                                                MAX(hist_values.bucket_count) AS bucket_count:int,
                                                MIN(hist_values.min_range) AS min_range:int,
                                                MAX(hist_values.max_range) AS max_range:int,
                                                MAX(hist_values.hist_type) AS hist_type:int,
                                                SUM(hist_values.count) AS sum_count:double,
                                                SUM(hist_values.sum) AS sum_sum:double,
                                                COUNT(hist_values) AS doc_count:long;

/* Join the results using cogroup because join operations require and equal number of rows and 
   here the name to name,value is a one to many relationship */
cogrpd = COGROUP hist_name_counts BY (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name),
                 hist_sums BY (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name);
flat = FOREACH cogrpd GENERATE FLATTEN(hist_sums), FLATTEN(hist_name_counts.doc_count) AS hist_name_doc_count;

/* Regroup and generate aggregate JSON objects */
grpd = GROUP flat BY (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id);
agg_jsons = FOREACH grpd GENERATE AggregateJson(group, flat) AS agg_json:chararray;

/* Store JSON objects into ElasticSearch with Wonderdog */
STORE agg_jsons INTO 'es://telemetry_agg_$start_date/data?json=true&size=100&tasks=4' 
                USING com.infochimps.elasticsearch.pig.ElasticSearchStorage('/home/xstevens/elasticsearch/elasticsearch-telemetry.yml',
                                                                            '/home/xstevens/elasticsearch/plugins');