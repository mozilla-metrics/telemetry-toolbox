register 'akela-0.3-SNAPSHOT.jar'
register 'telemetry-toolbox-0.1-SNAPSHOT.jar'

SET pig.logfile telemetry-sandbox.log;
SET default_parallel 8;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec lzo;

raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('$start_date', '$end_date', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray);
genmap = FOREACH raw GENERATE k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[];
/* note that this substring only works on double digits...change to regex if we want it to be more generic in the future */
filtered = FILTER genmap BY json_map#'info'#'appName' == 'Firefox' AND
                            SUBSTRING(json_map#'info'#'appVersion',0,2) == '$major_version';
buildids = FOREACH filtered GENERATE json_map#'info'#'platformBuildID' AS build_id:chararray;
grpd = GROUP buildids BY build_id;
counts = FOREACH grpd GENERATE group, COUNT(buildids);
store counts into 'buildidcounts-$major_version-$start_date-$end_date';