register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'wonderdog-1.0-SNAPSHOT.jar'
register 'elasticsearch/lib/0.19.3/*.jar'

SET pig.logfile telemetry_sandbox.log;
SET default_parallel 53;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define IsMap com.mozilla.pig.filter.map.IsMap();

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);



