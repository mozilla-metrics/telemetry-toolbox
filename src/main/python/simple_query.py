#!/usr/bin/env python

from subprocess import call
from optparse import OptionParser


def main():
    parser = OptionParser(usage="usage: %prog [options] query",
                          version="%prog 0.1")
    parser.add_option("-s", "--startdate",
                      help="start date to scan ")
    parser.add_option("-e", "--enddate",
                      help="end date to scan ")

    (options, args) = parser.parse_args()
    print options
    if not options.startdate:
        parser.error("startdate is not given")
    if not options.enddate:
        parser.error("enddate is not given")
    if args != None and len(args[0].split()) == 2:
        parser.error("please provide query as CYCLE_COLLECTOR > 3000")
    (json_key, sub_json_key,comparator, value) = parse_query(args[0])
    if sub_json_key == "range" :
        parser.error("Allowed sub keys are bucket_count,values,sum")


    cmd = "pig -f simple_query.pig -p start_date=%s -p end_date=%s -p json_key=%s -p sub_json_key=%s -p comparator='%s' -p value=%s" % (options.startdate ,options.enddate,json_key,sub_json_key,comparator,value)
    print cmd
    call(cmd,shell=True)


def parse_query(query) :
    query_split = query.split()
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


if __name__ == '__main__':
    main()
