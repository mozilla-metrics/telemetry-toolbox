register 'akela-0.4-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry_apc.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

define AddonPrivacyCorrection com.mozilla.telemetry.pig.eval.json.AddonPrivacyCorrection();

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
privacy_corrected = FOREACH raw GENERATE k, AddonPrivacyCorrection(json) AS new_json:chararray;
postfltrd = FILTER privacy_corrected BY new_json IS NOT NULL;
STORE postfltrd INTO 'hbase://telemetry' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json');