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
mainthreads = FOREACH filtered_main GENERATE FLATTEN(com.mozilla.telemetry.pig.eval.SlowSqlTuples(json_map#'slowSQL'#'mainThread'));
otherthreads = FOREACH filtered_other GENERATE FLATTEN(com.mozilla.telemetry.pig.eval.SlowSqlTuples(json_map#'slowSQL'#'otherThreads'));
STORE mainthreads INTO 'telemetry-slowsql-main' USING PigStorage();
STORE otherthreads INTO 'telemetry-slowsql-other' USING PigStorage();