register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-sandbox.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

oct = LOAD 'telemetry-gc-uptime-ratio-oct-11-31' AS (k:chararray,version:chararray,hist_name:chararray,v:chararray,count:double,sum:long,k2,uptime:int);
nov1 = LOAD 'telemetry-gc-uptime-ratio-nov-1-15' AS (k:chararray,version:chararray,hist_name:chararray,v:chararray,count:double,sum:long,k2,uptime:int);
nov2 = LOAD 'telemetry-gc-uptime-ratio-nov-16-30' AS (k:chararray,version:chararray,hist_name:chararray,v:chararray,count:double,sum:long,k2,uptime:int);
dec = LOAD 'telemetry-gc-uptime-ratio-dec-1-20' AS (k:chararray,version:chararray,hist_name:chararray,v:chararray,count:double,sum:long,k2,uptime:int);
u = UNION oct,nov1,nov2,dec;
filtered = FILTER u BY count > 0.0 AND uptime > 0.0;
rdata = FOREACH filtered GENERATE k,SUBSTRING(k,1,9) AS d,version,count,uptime;
grouped = GROUP rdata BY (k,d,version);
group_sums = FOREACH grouped GENERATE FLATTEN(group) AS (k,d,version),
                                      SUM(rdata.count) AS sum_count,
                                      FLATTEN(rdata.uptime) AS uptime;
dist = DISTINCT group_sums;
STORE dist INTO 'telemetry-gc-uptime-ratio';

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111011', '20111031', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY SUBSTRING(json_map#'info'#'appVersion', 0, 4) == '10.0';
histograms = FOREACH filtered_genmap GENERATE k,json_map#'info'#'appVersion' AS version:chararray,FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
filtered_histograms = FILTER histograms BY hist_name == 'GC_MS';
relevant_hist_fields = FOREACH filtered_histograms GENERATE k,version,hist_name,v,count,sum;
simple_measures = FOREACH filtered_genmap GENERATE k,json_map#'simpleMeasurements'#'uptime' AS uptime:int;
joined = JOIN relevant_hist_fields BY k,simple_measures BY k;
STORE joined INTO 'telemetry-gc-uptime-ratio-oct-11-31';



data = LOAD 'telemetry-gc-uptime-ratio-oct-11-31' AS (k:chararray,version:chararray,hist_name:chararray,v:chararray,count:double,sum:long,uptime:int);

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111111', '20111114', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered = FILTER genmap BY com.mozilla.pig.filter.map.ContainsKey(json_map#'histograms', 'MOZ_SQLITE_WEBAPPS_SYNC_MAIN_THREAD_MS') OR com.mozilla.pig.filter.map.RegexContainsKey(json_map#'histograms', '^STARTUP_.*');
json_only = FOREACH filtered GENERATE json;
STORE json_only INTO 'telemetry-export';

histograms = FOREACH genmap GENERATE k,FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
hist_name_json = FOREACH histograms GENERATE k,REGEX_EXTRACT(hist_name,'(^STARTUP_.*)',1) AS filter_hist_name,hist_name;
filtered = FILTER hist_name_json BY filter_hist_name IS NOT NULL OR hist_name == 'MOZ_SQLITE_WEBAPPS_SYNC_MAIN_THREAD_MS';
keys_only = FOREACH filtered GENERATE k;

FILTER raw BY k;
joined = JOIN filtered BY k LEFT OUTER,raw BY k PARALLEL 256;
jsons = FOREACH joined GENERATE filtered.k,raw.json;
STORE jsons INTO 'telemetry-export';

/* CC buckets */
raw = LOAD '$input' USING PigStorage() AS (d,product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name,v,bucket_count,min_range,max_range,hist_type,sum_count,doc_count,sum_sum);
filtered = FILTER raw BY product == 'Firefox' AND hist_name == 'CYCLE_COLLECTOR';
gendata = FOREACH filtered GENERATE product,product_version,hist_name,v;
grouped = GROUP gendata BY (product,product_version,hist_name);
dist_by_version = FOREACH grouped {
    dist = DISTINCT gendata.v;
    GENERATE group, dist;
}
dist_sizes = FOREACH dist_by_version GENERATE group,SIZE(dist);
STORE dist_sizes INTO '$output';

/* CYCLE_COLLECTOR bucket counts allowing sum_count = 0
69 cc-buckets-20111023.txt
61 cc-buckets-20111024.txt
68 cc-buckets-20111025.txt
83 cc-buckets-20111026.txt
50 cc-buckets-20111027.txt
86 cc-buckets-20111028.txt
72 cc-buckets-20111029.txt
50 cc-buckets-20111030.txt
83 cc-buckets-20111031.txt
69 cc-buckets-20111111.txt
65 cc-buckets-20111112.txt
86 cc-buckets-20111113.txt
58 cc-buckets-20111114.txt
72 cc-buckets-20111115.txt
50 cc-buckets-20111116.txt
*/

/* Suspect builds 

10/23
Firefox 7.0 (20110824172139,20110824172139) on WINNT 5.1
Firefox 7.0 (20110916091512,20110916091512) on WINNT 5.1
Firefox 7.0.1 (20110928134238,20110928134238) on WINNT 5.1
Firefox 7.0.1 (20110928134238,20110928134238) on WINNT 6.1
Firefox 8.0 (20111005184620,20111005184620) on WINNT 5.1
Firefox 8.0 (20111011182523,20111011182523) on WINNT 5.1 => EARLY_GLUESTARTUP_READ_TRANSFER 15 instead of 12

10/26
Firefox 7.0 (20110916091512,20110916091512) on WINNT 5.1
Firefox 7.0.1 (20110928134238,20110928134238) on WINNT 5.1
Firefox 7.0.1 (20110928134238,20110928134238) on WINNT 6.1
Firefox 8.0 (20111011182523,20111011182523) on WINNT 5.1

*/

/* This is part of investigating histograms with bad bucket values */
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111023', '20111023', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' AND 
                 json_map#'info'#'appVersion' == '8.0' AND
                 json_map#'info'#'appBuildID' == '20111011182523' AND 
                 json_map#'info'#'platformBuildID' == '20111011182523';
hist_values = FOREACH filtered_genmap GENERATE k, SUBSTRING(k,1,9) AS d:chararray, 
                                               (chararray)json_map#'info'#'appName' AS product:chararray,
                                               (chararray)json_map#'info'#'appVersion' AS product_version:chararray, 
                                               (chararray)json_map#'info'#'arch' AS arch:chararray,
                                               (chararray)json_map#'info'#'OS' AS os:chararray, 
                                               (chararray)json_map#'info'#'version' AS os_version:chararray,
                                               (chararray)json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                               (chararray)json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                               FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
/* 1 bad doc in there somewhere that uses different buckets */
bad_doc = FILTER hist_values BY hist_name == 'EARLY_GLUESTARTUP_READ_TRANSFER' AND (v == '6' or v == '19' or v == '59');
thekey = FOREACH bad_doc GENERATE k;

hist_by_name_and_v = GROUP hist_values BY (d,product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name,v,bucket_count,min_range,max_range,hist_type);
hist_sums = FOREACH hist_by_name_and_v GENERATE FLATTEN(group) AS (d,product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name,v,bucket_count,min_range,max_range,hist_type), 
                                                SUM(hist_values.count) AS sum_count, 
                                                COUNT(hist_values) AS doc_count, 
                                                SUM(hist_values.sum) AS sum_sum;
filtered_sums = FILTER hist_sums BY hist_name == 'EARLY_GLUESTARTUP_READ_TRANSFER';
ordered = ORDER filtered_sums BY d,product,product_version,arch,os,os_version,app_build_id,plat_build_id,hist_name;
STORE ordered INTO 'suspect-aggregation';

                                              
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111023', '20111023', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' AND 
                 json_map#'info'#'appVersion' == '8.0' AND
                 json_map#'info'#'OS' == '' AND
                 json_map#'info'#'version' == '' AND
                 json_map#'info'#'appBuildID' == '20111011182523' AND 
                 json_map#'info'#'platformBuildID' == '20111011182523' AND
                 json_map#'info'#'histograms'#'EARLY_GLUESTARTUP_READ_TRANSFER' IS NOT NULL AND
                 (double)SIZE(json_map#'info'#'histograms'#'EARLY_GLUESTARTUP_READ_TRANSFER'#'values') > (double)json_map#'info'#'histograms'#'EARLY_GLUESTARTUP_READ_TRANSFER'#'bucket_count';
keys = FOREACH filtered GENERATE k;
STORE keys INTO 'suspect-keys';
