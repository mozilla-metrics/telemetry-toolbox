register 'akela-0.5-SNAPSHOT.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'


SET pig.logfile telemetry_counts.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);


genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];

raw_group = GROUP raw ALL;
raw_counts = FOREACH raw_group generate COUNT(raw);
dump raw_counts;

validated_docs = FILTER genmap BY json_map#'info'#'validForSchema' == 'true';
validated_docs_group = GROUP validated_docs ALL;
validated_counts = FOREACH validated_docs_group generate COUNT(validated_docs);
dump validated_counts;
invalid_docs = FILTER genmap BY json_map#'info'#'validForSchema' == 'false';
invalid_docs_group = GROUP invalid_docs ALL;
invalid_counts = FOREACH invalid_docs_group generate COUNT(invalid_docs);
dump invalid_counts;