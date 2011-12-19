register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-slowsql.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_main = FILTER genmap BY json_map#'slowSQL'#'mainThread' IS NOT NULL;
filtered_other = FILTER genmap BY json_map#'slowSQL'#'otherThreads' IS NOT NULL;
mainthreads = FOREACH filtered_main GENERATE FLATTEN(com.mozilla.telemetry.pig.eval.SlowSqlTuples(json_map#'slowSQL'#'mainThread')) AS (sql:chararray, count:long, t:long);
otherthreads = FOREACH filtered_other GENERATE FLATTEN(com.mozilla.telemetry.pig.eval.SlowSqlTuples(json_map#'slowSQL'#'otherThreads')) AS (sql:chararray, count:long, t:long);

main_avg_less_than_100 = FILTER mainthreads BY (t / count) < 100;
other_avg_less_than_100 = FILTER otherthreads BY (t / count) < 100;

STORE mainthreads INTO 'slowsql-main';
STORE main_avg_less_than_100 INTO 'slowsql-main-avg-lessthan-100';

STORE otherthreads INTO 'slowsql-other';
STORE other_avg_less_than_100 INTO 'slowsql-other-avg-lessthan-100';