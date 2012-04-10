register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

SET pig.logfile telemetry-export.log;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define HistogramValueTuples com.mozilla.telemetry.pig.eval.HistogramValueTuples();

/*raw = LOAD 'hbase://telemetry' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('data:json','-loadKey=true -caching=100') AS (k:chararray,json:chararray);*/
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
/* modify based on your needs and what you're after */
filtered_genmap = FILTER genmap BY (json_map#'info'#'appName' == 'Firefox' OR 
                                   json_map#'info'#'appName' == 'Thunderbird' OR 
                                   json_map#'info'#'appName' == 'Fennec') AND
                                   (json_map#'info'#'reason' == 'idle-daily' OR json_map#'info'#'reason' == 'saved-session');
origdata = FOREACH filtered_genmap GENERATE k,json;

STORE origdata INTO 'telemetry-export' USING PigStorage();
