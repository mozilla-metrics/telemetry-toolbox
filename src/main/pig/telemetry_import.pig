register 'akela-0.5-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'
register 'elephant-bird-2.2.0.jar'

SET pig.logfile telemetry_import.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

%declare TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';
%declare SEQFILE_LOADER 'com.twitter.elephantbird.pig.load.SequenceFileLoader';

raw = LOAD '$input' USING $SEQFILE_LOADER (
    '-c $TEXT_CONVERTER', 
    '-c $TEXT_CONVERTER'
) AS (key: chararray, json: chararray);
STORE raw INTO 'hbase://$output_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json');