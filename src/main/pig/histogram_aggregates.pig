/* Aggregate all histograms in a single pass */
register './akela-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry-hist-aggregates.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

/*raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20111030', '20111030', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);*/
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
hist_values = FOREACH genmap GENERATE k,FLATTEN(com.mozilla.telemetry.pig.eval.HistogramValueTuples(json_map#'histograms')) AS (hist_name:chararray, v:chararray, count:double);
grouped_hist_values = GROUP hist_values BY (hist_name, v);
sums = FOREACH grouped_hist_values GENERATE FLATTEN(group) AS (hist_name, v), SUM(hist_values.count);
STORE sums INTO 'telemetry-histogram-aggregates-$start_date-$end_date' USING PigStorage();