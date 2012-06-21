register 'akela-0.4-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'elephant-bird-2.2.0.jar'

SET pig.logfile telemetry-import.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

%declare BYTES_CONVERTER 'com.twitter.elephantbird.pig.util.BytesWritableConverter';
%declare TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';
%declare SEQFILE_LOADER 'com.twitter.elephantbird.pig.load.SequenceFileLoader';

raw = LOAD '$input' USING $SEQFILE_LOADER (
    '-c $BYTES_CONVERTER', 
    '-c $TEXT_CONVERTER'
) AS (key: bytearray, json: chararray);
STORE raw INTO 'hbase://$output_table' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json');