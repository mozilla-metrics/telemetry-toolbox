/* Get the total number of submissions for each day and count them by product and version */
register 'akela-0.2-SNAPSHOT.jar'

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE SUBSTRING(k,1,9) AS d:chararray, com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
grouped = GROUP genmap BY (d,(chararray)json_map#'info'#'appName',(chararray)json_map#'info'#'appVersion');
group_counts = FOREACH grouped GENERATE FLATTEN(group) AS (d,product:chararray,product_version:chararray),COUNT(genmap);
ordered = ORDER group_counts BY product,product_version,d;
STORE ordered INTO 'telemetry-submissions-$start_date-$end_date' USING PigStorage();