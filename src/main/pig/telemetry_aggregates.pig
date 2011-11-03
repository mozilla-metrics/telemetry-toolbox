/* Aggregate all telemetry data in preparation for indexing the results using AggregateElasticSearchIndexer */
register 'akela-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry-aggregates.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/* raw = LOAD 'hbase://telemetry' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray); */
/* raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111030', '20111030', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray); */
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' OR json_map#'info'#'appName' == 'Thunderbird' OR json_map#'info'#'appName' == 'Fennec'; 
hist_values = FOREACH filtered_genmap GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                                               (chararray)json_map#'info'#'appName' AS product:chararray,
                                               (chararray)json_map#'info'#'appVersion' AS product_version:chararray, 
                                               (chararray)json_map#'info'#'arch' AS arch:chararray,
                                               (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               (chararray)json_map#'info'#'version' AS os_version:chararray,
                                               (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                               (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                               FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long);

simple_measures = FOREACH filtered_genmap GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                                               (chararray)json_map#'info'#'appName' AS product:chararray,
                                               (chararray)json_map#'info'#'appVersion' AS product_version:chararray, 
                                               (chararray)json_map#'info'#'arch' AS arch:chararray,
                                               (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               (chararray)json_map#'info'#'version' AS os_version:chararray,
                                               (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                               (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                               FLATTEN(com.mozilla.telemetry.pig.eval.SimpleMeasureTuples(json_map#'simpleMeasurements')) AS (hist_name:chararray, v:chararray, count:double, sum:long);

unified = UNION hist_values,simple_measures;
by_name_and_v = GROUP unified BY (d,product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name,v);
sums = FOREACH by_name_and_v GENERATE FLATTEN(group) AS (d,product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name,v), SUM(unified.count) AS sum_count, COUNT(unified) AS doc_count, SUM(unified.sum) AS sum_sum;

STORE sums INTO 'telemetry-aggregates-$start_date-$end_date' USING PigStorage();

/* Localhost example for my own testing (not exactly the same as above)*/
/*
register '/Users/xstevens/workspace/akela/target/akela-0.2-SNAPSHOT.jar'
raw = LOAD 'file:///Users/xstevens/Desktop/telemetry-nonpretty.js' AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
hist_values = FOREACH genmap GENERATE (chararray)json_map#'info'#'appName' AS product:chararray,
                                      (chararray)json_map#'info'#'appVersion' AS product_version:chararray, 
                                      (chararray)json_map#'info'#'arch' AS arch:chararray,
                                      (chararray)json_map#'info'#'OS' AS os:chararray, 
                                      (chararray)json_map#'info'#'version' AS os_version:chararray,
                                      (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                      (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                      FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double);
by_name_and_v = GROUP hist_values BY (product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name,v);                                                           
sums = FOREACH by_name_and_v GENERATE FLATTEN(group) AS (product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name,v), SUM(hist_values.count) AS sum_count;

simple_measures = FOREACH genmap GENERATE (chararray)json_map#'info'#'appName' AS product:chararray,
                                               (chararray)json_map#'info'#'appVersion' AS product_version:chararray, 
                                               (chararray)json_map#'info'#'arch' AS arch:chararray,
                                               (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               (chararray)json_map#'info'#'version' AS os_version:chararray,
                                               (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                               (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                               (long)json_map#'simpleMeasurements'#'uptime' AS uptime:long,
                                               (long)json_map#'simpleMeasurements'#'main' AS main:long,
                                               (long)json_map#'simpleMeasurements'#'firstPaint' AS first_paint:long,
                                               (long)json_map#'simpleMeasurements'#'sessionRestored' AS session_restored:long;
grouped_measures = GROUP simple_measures BY (product,product_version,arch,os,os_version,app_build_id,plat_build_id);
avgs = FOREACH grouped_measures GENERATE FLATTEN(group) AS (product,product_version,arch,os,os_version,app_build_id,plat_build_id), AVG(simple_measures.uptime) AS avg_uptime, AVG(simple_measures.main) AS avg_main, AVG(simple_measures.first_paint) AS avg_first_paint, AVG(simple_measures.session_restored) AS avg_session_restored;
results = JOIN avgs BY (product,product_version,arch,os,os_version,app_build_id,plat_build_id) FULL OUTER, sums BY (product,product_version,arch,os,os_version,app_build_id,plat_build_id);
final_results = FOREACH results GENERATE avgs::product,avgs::product_version,avgs::arch,avgs::os,avgs::os_version,avgs::app_build_id,avgs::plat_build_id,avgs::avg_uptime,avgs::avg_main,avgs::avg_first_paint,avgs::avg_session_restored,sums::hist_name,sums::v,sums::sum_count;
*/

/* 54515 combos for 10/27 */
/*
combos = FOREACH filtered_genmap GENERATE json_map#'info'#'appName' AS product:chararray, 
                                 json_map#'info'#'appVersion' AS product_version:chararray, 
                                 json_map#'info'#'arch' AS arch:chararray,
                                 json_map#'info'#'OS' AS os:chararray, 
                                 json_map#'info'#'version' AS os_version:chararray,
                                 json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                 json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                 json_map#'info'#'memsize' AS mem_size:int,
                                 json_map#'info'#'cpucount' AS cpu_count:int,
                                 json_map#'info'#'reason' AS submission_reason:chararray;
dcombos = DISTINCT combos;
STORE dcombos INTO 'telemetry-filter-combos' USING PigStorage();
*/

/* 4244 combos for 10/27 */
/*
combos = FOREACH filtered_genmap GENERATE json_map#'info'#'appName' AS product:chararray, 
                                          json_map#'info'#'appVersion' AS product_version:chararray, 
                                          json_map#'info'#'arch' AS arch:chararray,
                                          json_map#'info'#'OS' AS os:chararray, 
                                          json_map#'info'#'version' AS os_version:chararray,
                                          json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                          json_map#'info'#'platformBuildID' AS plat_build_id:chararray;
dcombos = DISTINCT combos;
STORE dcombos INTO 'telemetry-filter-combos' USING PigStorage();
*/