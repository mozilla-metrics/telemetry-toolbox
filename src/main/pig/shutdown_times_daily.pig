register '/usr/lib/hbase/lib/zookeeper.jar'
register '/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar'
register 'akela-0.4-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile simple_query.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define SingleHistogramTuple com.mozilla.telemetry.pig.eval.SingleHistogramTuple();

define CompareJsonValue1 com.mozilla.telemetry.pig.eval.json.TelemetryValueCompare('shutdownDuration','values','!=','null'); 
filter_raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('20130410', '20130410', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray); 
filter_raw = FILTER filter_raw by CompareJsonValue1(json); 
filter_raw = SAMPLE filter_raw 0.25; 
filter_raw = FOREACH filter_raw generate k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[]; 
filter_raw = FOREACH filter_raw generate json_map#'simpleMeasurements'#'shutdownDuration',json_map#'info'#'OS',json_map#'info'#'appVersion',json_map#'info'#'appBuildID'; 
STORE filter_raw INTO  'shutdown_times_20130410' using PigStorage(',');