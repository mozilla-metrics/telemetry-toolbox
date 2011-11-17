register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-sandbox.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

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