/* Aggregate all telemetry data in preparation for indexing the results using AggregateElasticSearchIndexer */
register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-aggregates.log;
SET default_parallel 16;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/* raw = LOAD 'hbase://telemetry' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray); */
/* raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111030', '20111030', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray); */
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY (json_map#'info'#'appName' == 'Firefox' OR 
                                   json_map#'info'#'appName' == 'Thunderbird' OR 
                                   json_map#'info'#'appName' == 'Fennec') AND
                                   (json_map#'info'#'reason' == 'idle-daily' OR json_map#'info'#'reason' == 'saved-session'); 
hist_values = FOREACH filtered_genmap GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                                               (chararray)json_map#'info'#'appName' AS product:chararray,
                                               (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                                               (chararray)json_map#'info'#'appUpdateChannel' AS product_channel:chararray, 
                                               (chararray)json_map#'info'#'arch' AS arch:chararray,
                                               (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               (chararray)json_map#'info'#'version' AS os_version:chararray,
                                               (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                               (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                               FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);

hist_by_name_and_v = GROUP hist_values BY (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name,v,bucket_count,min_range,max_range,hist_type);
hist_sums = FOREACH hist_by_name_and_v GENERATE FLATTEN(group) AS (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name,v,bucket_count,min_range,max_range,hist_type), 
                                                SUM(hist_values.count) AS sum_count, 
                                                COUNT(hist_values) AS doc_count, 
                                                SUM(hist_values.sum) AS sum_sum;
                                               
simple_measures = FOREACH filtered_genmap GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                                                   (chararray)json_map#'info'#'appName' AS product:chararray,
                                                   (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                                                   (chararray)json_map#'info'#'appUpdateChannel' AS product_channel:chararray, 
                                                   (chararray)json_map#'info'#'arch' AS arch:chararray,
                                                   (chararray)json_map#'info'#'OS' AS os:chararray, 
                                                   (chararray)json_map#'info'#'version' AS os_version:chararray,
                                                   (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                                   (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                                   FLATTEN(com.mozilla.telemetry.pig.eval.SimpleMeasureTuples(json_map#'simpleMeasurements')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);

sm_by_name_and_v = GROUP simple_measures BY (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name,v,bucket_count,min_range,max_range,hist_type);
sm_sums = FOREACH sm_by_name_and_v GENERATE FLATTEN(group) AS (d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id,hist_name,v,bucket_count,min_range,max_range,hist_type), 
                                            SUM(simple_measures.count) AS sum_count, 
                                            COUNT(simple_measures) AS doc_count, 
                                            SUM(simple_measures.sum) AS sum_sum;

unified = UNION hist_sums,sm_sums;
ordered = ORDER unified BY d,product,product_version,product_channel,arch,os,os_version,app_build_id,plat_build_id;
STORE ordered INTO 'telemetry-aggregates-$start_date-$end_date' USING PigStorage();