/* Aggregate all telemetry data in preparation for indexing the results using AggregateElasticSearchIndexer */
register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-startupinterrupted.log;
SET default_parallel ;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' AND json_map#'info'#'OS' == 'WINNT' AND (json_map#'simpleMeasurements'#'startupInterrupted' == 0 OR json_map#'simpleMeasurements'#'startupInterrupted' == 1); 
startup_times = FOREACH filtered_genmap GENERATE json_map#'info'#'appVersion' AS product_version,
                                                 json_map#'info'#'OS' AS os,
                                                 json_map#'info'#'version' AS os_version,
                                                 (int)json_map#'simpleMeasurements'#'startupInterrupted' AS startup_interrupted:int,
                                                 (long)json_map#'simpleMeasurements'#'firstPaint' AS firstpaint:long,
                                                 (long)json_map#'simpleMeasurements'#'sessionRestored' AS sessionrestored:long;
ordered = ORDER startup_times BY product_version,os,os_version;
STORE startup_times INTO 'telemetry-startupinterrupted-$start_date-$end_date' USING PigStorage();


