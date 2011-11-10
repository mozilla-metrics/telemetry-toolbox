/* Aggregate all telemetry data in preparation for indexing the results using AggregateElasticSearchIndexer */
register 'akela-0.2-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-combos.log;
SET default_parallel 8;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
filtered_genmap = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' OR json_map#'info'#'appName' == 'Thunderbird' OR json_map#'info'#'appName' == 'Fennec';

/* 54515 combos for 10/27 */
/*
combos = FOREACH filtered_genmap GENERATE json_map#'info'#'appName' AS product:chararray, 
                                 json_map#'info'#'appVersion' AS product_version:chararray, 
                                 json_map#'info'#'arch' AS arch:chararray,
                                 json_map#'info'#'OS' AS os:chararray, 
                                 json_map#'info'#'version' AS os_version:chararray,
                                 json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                 json_map#'info'#'platformBuildID' AS plat_build_id:chararray,
                                 json_map#'info'#'memsize' AS mem_size:int,
                                 json_map#'info'#'cpucount' AS cpu_count:int,
                                 json_map#'info'#'reason' AS submission_reason:chararray;
dcombos = DISTINCT combos;
STORE dcombos INTO 'telemetry-filter-combos' USING PigStorage();
*/

/* 4244 combos for 10/27 */
combos = FOREACH filtered_genmap GENERATE json_map#'info'#'appName' AS product:chararray, 
                                          json_map#'info'#'appVersion' AS product_version:chararray, 
                                          json_map#'info'#'arch' AS arch:chararray,
                                          json_map#'info'#'OS' AS os:chararray, 
                                          json_map#'info'#'version' AS os_version:chararray,
                                          json_map#'info'#'appBuildID' AS app_build_id:chararray,
                                          json_map#'info'#'platformBuildID' AS plat_build_id:chararray;
dcombos = DISTINCT combos;
STORE dcombos INTO 'telemetry-filter-combos' USING PigStorage();