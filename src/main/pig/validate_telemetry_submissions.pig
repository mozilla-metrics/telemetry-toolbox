register 'akela-0.4-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile validate-telemetry-submissions.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define ValidateTelemetrySubmission com.mozilla.telemetry.pig.eval.json.ValidateTelemetrySubmission('telemetry_spec_lookup.properties');

raw = LOAD 'hbase://$input_table' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);

validated_docs = foreach raw generate k, ValidateTelemetrySubmission(*) AS json;
filter_nulls = filter validated_docs by json is not null;

STORE filter_nulls INTO 'hbase://$output_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json');
