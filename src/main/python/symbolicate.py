#!/usr/bin/env python
# encoding: utf-8
"""
symbolicate.py
Copyright (c) 2012 Mozilla Foundation. All rights reserved.
"""

import sys
import getopt
import json
import sys
import urllib2
import codecs
import re
import gzip

help_message = '''
    Takes chrome hangs list of memory addresses from JSON dumps and converts them to stack traces
    Required:
        -i, --input <input_file>
        -o, --output <output_file>
    Optional:
        -h, --help
'''

SYMBOL_SERVER_URL = "http://symbolapi.mozilla.org:80/"

# Pulled this method from Vladan's code
def symbolicate(chromeHangsObj):
    if isinstance(chromeHangsObj, list):
        version = 1
        requestObj = chromeHangsObj
        numStacks = len(chromeHangsObj)
        if numStacks == 0:
            return []
    else:
        numStacks = len(chromeHangsObj["stacks"])
        if numStacks == 0:
            return []
        if len(chromeHangsObj["memoryMap"][0]) == 2:
            version = 3
        else:
            assert len(chromeHangsObj["memoryMap"][0]) == 4
            version = 2
        requestObj = {"stacks"    : chromeHangsObj["stacks"],
                      "memoryMap" : chromeHangsObj["memoryMap"],
                      "version"   : version}
    try:
        requestJson = json.dumps(requestObj)
        headers = { "Content-Type": "application/json" }
        requestHandle = urllib2.Request(SYMBOL_SERVER_URL, requestJson, headers)
        response = urllib2.urlopen(requestHandle)
    except Exception as e:
        sys.stderr.write("Exception while forwarding request: " + str(e) + "\n")
        return []
    try:
        responseJson = response.read()
    except Exception as e:
        sys.stderr.write("Exception while reading server response to symbolication request: " + str(e) + "\n")
        return []
    
    try:
        responseSymbols = json.loads(responseJson)
        # Sanity check
        if numStacks != len(responseSymbols):
            sys.stderr.write(str(len(responseSymbols)) + " hangs in response, " + str(numStacks) + " hangs in request!\n")
            return []
        
        # Sanity check
        for hangIndex in range(0, numStacks):
            if version == 1:
                stack = chromeHangsObj[hangIndex]["stack"]
            else:
                stack = chromeHangsObj["stacks"][hangIndex]
            requestStackLen = len(stack)
            responseStackLen = len(responseSymbols[hangIndex])
            if requestStackLen != responseStackLen:
                sys.stderr.write(str(responseStackLen) + " symbols in response, " + str(requestStackLen) + " PCs in request!\n")
                return []
    except Exception as e:
        sys.stderr.write("Exception while parsing server response to forwarded request: " + str(e) + "\n")
        return []
    
    return responseSymbols

def process(input_file, output_file):
    fin = open(input_file, "r")
    fout = gzip.open(output_file, "wb")
    for line in fin:
        splits = line.split("\t");
        try:
            json_dict = json.loads(splits[1])

            hang_stacks = []
            hangs = json_dict.get("chromeHangs")
            if hangs:
              del json_dict["chromeHangs"]
              hang_stacks = symbolicate(hangs)

            late_writes_stacks = []
            writes = json_dict.get("lateWrites")
            if writes:
              late_writes_stacks = symbolicate(writes)
              del json_dict["lateWrites"]

            del json_dict["histograms"]
            fout.write(splits[0].rstrip())
            fout.write("\t")
            fout.write(json.dumps(json_dict))

            for stack in hang_stacks:
                fout.write("\n----- BEGIN HANG STACK -----\n")
                fout.write("\n".join(stack))
                fout.write("\n----- END HANG STACK -----\n")

            for stack in late_writes_stacks:
                fout.write("\n----- BEGIN LATE WRITE STACK -----\n")
                fout.write("\n".join(stack))
                fout.write("\n----- END LATE WRITE STACK -----\n")

        except Exception as e:
            sys.stderr.write("Exception while processing json item: " + str(e) + "\n")
    fin.close()
    fout.close()  

class Usage(Exception):
	def __init__(self, msg):
		self.msg = msg


def main(argv=None):
	if argv is None:
		argv = sys.argv
	try:
		try:
			opts, args = getopt.getopt(argv[1:], "hi:o:v", ["help", "input=", "output="])
		except getopt.error, msg:
			raise Usage(msg)
		
		input_file = None
		output_file = None
		# option processing
		for option, value in opts:
			if option == "-v":
				verbose = True
			if option in ("-h", "--help"):
				raise Usage(help_message)
			if option in ("-i", "--input"):
			    input_file = value
			if option in ("-o", "--output"):
				output_file = value
		process(input_file, output_file)
	except Usage, err:
	    print >> sys.stderr, sys.argv[0].split("/")[-1] + ": " + str(err.msg)
	    print >> sys.stderr, " for help use --help"
	    return 2

if __name__ == "__main__":
	sys.exit(main())
