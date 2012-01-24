register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-sandbox.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/*-param start_date=20110927 -param end_date=20110930*/
/*-param start_date=20111001 -param end_date=20111015*/
/*-param start_date=20111016 -param end_date=20111031*/
/*-param start_date=20111101 -param end_date=20111108*/
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' AND 
                                   SUBSTRING(json_map#'info'#'appVersion', 0, 4) == '10.0' AND
                                   (json_map#'info'#'OS' == 'Darwin' OR json_map#'info'#'OS' == 'Linux');
histograms = FOREACH filtered_genmap GENERATE k,SUBSTRING(k,1,9) AS d:chararray,
                                              json_map#'info'#'appVersion' AS version:chararray,
                                              json_map#'info'#'OS' AS os:chararray,
                                              FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double, sum:long, bucket_count:int, min_range:int, max_range:int, hist_type:int);
filtered_histograms = FILTER histograms BY hist_name == 'CYCLE_COLLECTOR' AND count > 0.0;
relevant_hist_fields = FOREACH filtered_histograms GENERATE k,d,version,os,hist_name,v,count,sum;
STORE relevant_hist_fields INTO 'telemetry-cc-$start_date-$end_date';