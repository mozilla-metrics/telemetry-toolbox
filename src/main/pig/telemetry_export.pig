register 'akela-0.5-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'
register 'elephant-bird-2.2.0.jar'

SET pig.logfile telemetry_export.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

%declare BYTES_CONVERTER 'com.twitter.elephantbird.pig.util.BytesWritableConverter';
%declare TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';
%declare SEQFILE_STORAGE 'com.twitter.elephantbird.pig.store.SequenceFileStorage';

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:bytearray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];

/* Keep all data for channels other than release */
filtered_prerelease = FILTER genmap BY json_map#'info'#'appUpdateChannel' == 'nightly' OR
                                       json_map#'info'#'appUpdateChannel' == 'aurora' OR
                                       json_map#'info'#'appUpdateChannel' == 'beta';
prerelease_data = FOREACH filtered_prerelease GENERATE k,json;
STORE prerelease_data INTO '$output-prerelease' USING $SEQFILE_STORAGE (
  '-c $BYTES_CONVERTER', '-c $TEXT_CONVERTER'
);

/* Sample release data */
filtered_release = FILTER genmap BY json_map#'info'#'appUpdateChannel' == 'release';
sample_filtered_release = SAMPLE filtered_release 0.1;
sampled_release_data = FOREACH sample_filtered_release GENERATE k,json;
STORE sampled_release_data INTO '$output-release' USING $SEQFILE_STORAGE (
  '-c $BYTES_CONVERTER', '-c $TEXT_CONVERTER'
);

/*
Unioned here seems to cause issues with Pig. Guessing it has something to do with
Pig type escalation and bytearrays. It's convinced this is an ArrayList
*/
/*
unioned = UNION prerelease_data, sampled_release_data;

STORE unioned INTO '$output-release' USING $SEQFILE_STORAGE (
  '-c $BYTES_CONVERTER', '-c $TEXT_CONVERTER'
);
*/
