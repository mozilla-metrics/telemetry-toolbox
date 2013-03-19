/*

Count the number of telemetry submissions by Firefox Version in the given date range.

Parameters:
  start_date - beginning of the date range, formatted YYYYMMDD
  end_date   - end of the date range, formatted YYYYMMDD

Example: to generate the counts for a specific day, Feb. 20, 2013:
  $ /path/to/pig -param start_date=20130220 -param end_date=20130220 telemetry_example.pig

*/
register 'akela-0.5-SNAPSHOT.jar'
register 'telemetry-toolbox-0.2-SNAPSHOT.jar'
register 'jackson-core-2.1.1.jar'
register 'jackson-databind-2.1.1.jar'
register 'jackson-annotations-2.1.1.jar'

SET pig.logfile telemetry-example.log;
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

define IsMap com.mozilla.pig.filter.map.IsMap();

/* Load data from hbase */
raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:bytearray, json:chararray);

/* Parse json payload */
genmap = FOREACH raw GENERATE k, com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];

/* Filter records with the expected structure and appName */
filtered_genmap = FILTER genmap BY json_map#'info' IS NOT NULL
                                   AND IsMap(json_map#'info')
                                   AND json_map#'info'#'appName' == 'Firefox'
                                   AND json_map#'info'#'appVersion' IS NOT NULL;

/* Extract the app version of each record */
fx_by_version = FOREACH filtered_genmap GENERATE k, json_map#'info'#'appVersion' AS fx_version:chararray;

/* Group records by app version */
grouped_versions = GROUP fx_by_version BY (fx_version);

/* Get the counts for each version */
counted_versions = FOREACH grouped_versions GENERATE group, COUNT(fx_by_version);

/* Save the counts into hdfs */
STORE counted_versions INTO 'telemetry-example-$start_date-$end_date';
