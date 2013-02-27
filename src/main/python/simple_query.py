#!/usr/bin/env python

from subprocess import call
from optparse import OptionParser
import re,os.path


def main():
    parser = OptionParser(usage="usage: %prog [options] query",
                          version="%prog 0.1")
    parser.add_option("-n","--filename",
                      help="filename for generated pig code to store")
    parser.add_option("-s", "--startdate",
                      help="start date to scan ")
    parser.add_option("-e", "--enddate",
                      help="end date to scan ")

    parser.add_option("-o", "--output",
                      help="output file",
                      default="stdout")

    parser.add_option("-a","--sample",
                      help="sample data");

    parser.add_option("-f","--fields",
                     help="output only given fields not entire json")
    parser.add_option("-d","--dryrun",
                     help="prints script")

    (options, args) = parser.parse_args()
    if not options.startdate:
        parser.error("startdate is not given")
    if not options.enddate:
        parser.error("enddate is not given")
    if not options.filename:
        parser.error("please provide file name to store generated pig code")
    if args != None and len(args[0].split()) < 2:
        parser.error("please provide query as CYCLE_COLLECTOR > 3000")

    construct_pig_script(args[0],options)
    run_pig_script(options.filename)

def run_pig_script(filename):
    if (os.path.exists(filename)):
        call("pig -f "+filename, shell=True)
    else:
        print "Unable to find "+filename+", check if the file created"
        raise

def construct_pig_script(query,options):
    pig_script = construct_pig_headers()
    pig_fetch_raw = construct_pig_fetch_query(options.startdate,options.enddate)
    (pig_defines, pig_filter)= construct_pig_filter_query(query)
    pig_script += pig_defines
    pig_script += pig_fetch_raw
    pig_script += pig_filter

    if options.sample:
        pig_script += construct_pig_sample(options.sample)

    if options.fields:
        pig_script += construct_pig_fetch_fields(options.fields)

    pig_script += construct_pig_output(options.output)
    write_to_file(pig_script,options.filename)

def write_to_file(pig_script,file_name):
    f = open(file_name,"w")
    f.write(pig_script)


def construct_pig_headers():
    return """
    register 'akela-0.4-SNAPSHOT.jar'
    register 'telemetry-toolbox-0.2-SNAPSHOT.jar'

    SET pig.logfile simple_query.log;
    SET pig.tmpfilecompression true;
    SET pig.tmpfilecompression.codec lzo;
    SET mapred.compress.map.output true;
    SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

    define SingleHistogramTuple com.mozilla.telemetry.pig.eval.SingleHistogramTuple();

    """

def construct_pig_sample(sample):
    return "filter_raw = SAMPLE filter_raw "+sample+"; \n"

def construct_pig_fetch_query(start_date,end_date):
    return "raw = LOAD 'hbase://telemetry' USING com.mozilla.pig.load.HBaseMultiScanLoader('"+start_date+"', '"+end_date+"', 'yyyyMMdd', 'data:json') AS (k:chararray, json:chararray); \n"

def construct_pig_filter_query(query):
    query_split = query.split("and")
    pig_defines = ""
    methods = []
    pig_filter = "filter_raw = FILTER raw by "
    count = 1
    for q in query_split:
        (json_key, sub_json_key,comparator, value) = parse_query(q)
        method = "CompareJsonValue"+str(count)
        count = count + 1
        pig_defines += "define "+method+" com.mozilla.telemetry.pig.eval.json.TelemetryValueCompare('"+json_key+"','"+sub_json_key+"','"+comparator+"','"+value+"'); \n";
        methods.append(method+"(json)");

    pig_filter += " and ".join(methods)
    pig_filter += "; \n"
    return (pig_defines, pig_filter)

def construct_pig_output(output):
    if output == "stdout":
        return "dump filter_raw;";
    else:
        return "STORE filter_raw INTO  '"+output+"' using PigStorage(',');"


def construct_pig_fetch_fields(fields):
    field_list = fields.split(",")
    if len(field_list) == 0:
        return
    pig_fields = "filter_raw = FOREACH filter_raw generate k,com.mozilla.pig.eval.json.JsonMap(json) AS json_map:map[]; \n"
    pig_fields += "filter_raw = FOREACH filter_raw generate "
    pig_map = []
    for field in field_list:
        if "." in field:
            count_dot = field.count(".")
            field_pig_map = field.replace(".","'#'")
            field_pig_map = "#'"+field_pig_map+"'"
            field_pig_map = "json_map"+field_pig_map
            if count_dot == 1 and "histograms" in field_pig_map:
                pig_map.append(" FLATTEN(SingleHistogramTuple("+field_pig_map+")) AS (hist_name:chararray, v:chararray,count:double, sum:long, bucket_count:int, min_range:int,max_range:int,hist_type:int,is_valid:int)")
            else:
                pig_map.append(field_pig_map)

        else:
            pig_map.append("json_map#'"+field+"'")

    pig_fields += ','.join(pig_map)
    pig_fields += "; \n"
    return pig_fields


def parse_query(query) :
    query_split = split_query(query)
    json_keys = query_split[0].split('.')

    if (len(json_keys)) == 2:
        json_key = json_keys[0]
        sub_json_key = json_keys[1]
    else:
        json_key = json_keys[0]
        sub_json_key = "values"
    comparator = query_split[1]
    value = query_split[2]
    return (json_key,sub_json_key,comparator,value)

def split_query(query):
    return [x.strip(' ') for x in re.split('(<=|>=|!=|=|>|<)',query)]

if __name__ == '__main__':
    main()
