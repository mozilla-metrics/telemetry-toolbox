/* Get the total number of submissions for each day and count them by product and version */
register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-submissions.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE SUBSTRING(k,1,9) AS d:chararray, 
                              com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[], 
                              com.mozilla.pig.eval.Size(json) AS json_size:long;
sub = FOREACH genmap GENERATE d, 
                              (chararray)json_map#'info'#'appName' AS product:chararray, 
                              (chararray)json_map#'info'#'appVersion' AS product_version:chararray,
                              json_size;                        
grouped = GROUP sub BY (d,product,product_version);
group_stats = FOREACH grouped GENERATE FLATTEN(group) AS (d,product:chararray,product_version:chararray), 
                                       COUNT(sub) AS count:long, 
                                       SUM(sub.json_size) AS sum_size:long,
                                       AVG(sub.json_size) AS avg_size:double;
ordered = ORDER group_stats BY product,product_version,d,count;
STORE ordered INTO 'telemetry-submissions-$start_date-$end_date' USING PigStorage();