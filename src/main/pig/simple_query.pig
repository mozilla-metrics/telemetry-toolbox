register 'akela-0.4-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile simple_query.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define CompareJsonValue com.mozilla.telemetry.pig.eval.json.TelemetryValueCompare('$json_key','$sub_json_key',
                                                                '$comparator','$value');

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);

cycle_collector = FILTER raw by CompareJsonValue(json);
dump cycle_collector;