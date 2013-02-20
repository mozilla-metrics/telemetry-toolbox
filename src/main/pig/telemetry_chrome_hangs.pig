/* Aggregate all telemetry data for a given day and index the aggregate json objects in ElasticSearch */
register 'akela-0.5-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'jackson-core-2.0.6.jar'
register 'jackson-databind-2.0.6.jar'
register 'jackson-annotations-2.0.6.jar'

SET pig.logfile telemetry-chrome-hangs.log;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define IsMap com.mozilla.pig.filter.map.IsMap();
define Size com.mozilla.pig.eval.Size();

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);

genmap = FOREACH raw GENERATE k,json,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];

filtered_genmap = FILTER genmap BY (json_map#'chromeHangs' IS NOT NULL
                                    AND IsMap(json_map#'chromeHangs')
                                    AND json_map#'chromeHangs'#'memoryMap' IS NOT NULL
                                    AND Size(json_map#'chromeHangs'#'memoryMap') > 0)
                                OR (json_map#'lateWrites' IS NOT NULL
                                    AND IsMap(json_map#'lateWrites')
                                    AND json_map#'lateWrites'#'memoryMap' IS NOT NULL
                                    AND Size(json_map#'lateWrites'#'memoryMap') > 0);

orig_data = FOREACH filtered_genmap GENERATE k,json;
STORE orig_data INTO 'chrome-hangs-$start_date-$end_date';
