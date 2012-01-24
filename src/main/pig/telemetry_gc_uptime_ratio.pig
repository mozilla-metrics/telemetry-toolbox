register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-sandbox.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111011', '20111031', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY SUBSTRING(json_map#'info'#'appVersion', 0, 4) == '10.0';
histograms = FOREACH filtered_genmap GENERATE k,json_map#'info'#'appVersion' AS version:chararray,FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
filtered_histograms = FILTER histograms BY hist_name == 'GC_MS';
relevant_hist_fields = FOREACH filtered_histograms GENERATE k,version,hist_name,v,count,sum;
simple_measures = FOREACH filtered_genmap GENERATE k,json_map#'simpleMeasurements'#'uptime' AS uptime:int;
joined = JOIN relevant_hist_fields BY k,simple_measures BY k;
STORE joined INTO 'telemetry-gc-uptime-ratio-oct-11-31';

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111011', '20111031', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY SUBSTRING(json_map#'info'#'appVersion', 0, 4) == '10.0';
histograms = FOREACH filtered_genmap GENERATE k,json_map#'info'#'appVersion' AS version:chararray,FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
filtered_histograms = FILTER histograms BY hist_name == 'GC_MS';
relevant_hist_fields = FOREACH filtered_histograms GENERATE k,version,hist_name,v,count,sum;
simple_measures = FOREACH filtered_genmap GENERATE k,json_map#'simpleMeasurements'#'uptime' AS uptime:int;
joined = JOIN relevant_hist_fields BY k,simple_measures BY k;
STORE joined INTO 'telemetry-gc-uptime-ratio-oct-11-31';

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111011', '20111031', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY SUBSTRING(json_map#'info'#'appVersion', 0, 4) == '10.0';
histograms = FOREACH filtered_genmap GENERATE k,json_map#'info'#'appVersion' AS version:chararray,FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
filtered_histograms = FILTER histograms BY hist_name == 'GC_MS';
relevant_hist_fields = FOREACH filtered_histograms GENERATE k,version,hist_name,v,count,sum;
simple_measures = FOREACH filtered_genmap GENERATE k,json_map#'simpleMeasurements'#'uptime' AS uptime:int;
joined = JOIN relevant_hist_fields BY k,simple_measures BY k;
STORE joined INTO 'telemetry-gc-uptime-ratio-oct-11-31';

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111011', '20111031', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY SUBSTRING(json_map#'info'#'appVersion', 0, 4) == '10.0';
histograms = FOREACH filtered_genmap GENERATE k,json_map#'info'#'appVersion' AS version:chararray,FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
filtered_histograms = FILTER histograms BY hist_name == 'GC_MS';
relevant_hist_fields = FOREACH filtered_histograms GENERATE k,version,hist_name,v,count,sum;
simple_measures = FOREACH filtered_genmap GENERATE k,json_map#'simpleMeasurements'#'uptime' AS uptime:int;
joined = JOIN relevant_hist_fields BY k,simple_measures BY k;
STORE joined INTO 'telemetry-gc-uptime-ratio-oct-11-31';

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

