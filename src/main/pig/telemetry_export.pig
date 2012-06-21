register 'akela-0.4-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'elephant-bird-2.2.0.jar'

SET pig.logfile telemetry-export.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

%declare BYTES_CONVERTER 'com.twitter.elephantbird.pig.util.BytesWritableConverter';
%declare TEXT_CONVERTER 'com.twitter.elephantbird.pig.util.TextConverter';
%declare SEQFILE_STORAGE 'com.twitter.elephantbird.pig.store.SequenceFileStorage';

/*raw = LOAD 'hbase://telemetry' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);*/
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:bytearray, json:chararray);
/*STORE raw INTO 'hdfs://admin1.mango.metrics.scl3.mozilla.com:8020/user/xstevens/telemetry-export-$start_date-$end_date';*/
STORE raw INTO '$output' USING $SEQFILE_STORAGE (
  '-c $BYTES_CONVERTER', '-c $TEXT_CONVERTER'
);
