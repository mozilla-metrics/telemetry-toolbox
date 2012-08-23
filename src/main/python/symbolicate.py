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
    try:
        requestJson = json.dumps(chromeHangsObj)
        headers = { "Content-Type": "application/json" }
        requestHandle = urllib2.Request(SYMBOL_SERVER_URL, requestJson, headers)
        response = urllib2.urlopen(requestHandle)
    except Exception as e:
        sys.stderr.write("Exception while forwarding request: " + str(e) + "\n")
        return None
    try:
        responseJson = response.read()
    except Exception as e:
        sys.stderr.write("Exception while reading server response to symbolication request: " + str(e) + "\n")
        return None
    
    try:
        responseSymbols = json.loads(responseJson)
        # Sanity check
        if len(chromeHangsObj) != len(responseSymbols):
            sys.stderr.write(str(len(responseSymbols)) + " hangs in response, " + str(len(chromeHangsObj)) + " hangs in request!\n")
            return None
        
        # Sanity check
        for hangIndex in range(0, len(chromeHangsObj)):
            requestStackLen = len(chromeHangsObj[hangIndex]["stack"])
            responseStackLen = len(responseSymbols[hangIndex])
            if requestStackLen != responseStackLen:
                sys.stderr.write(str(len(responseStackLen)) + " symbols in response, " + str(len(requestStackLen)) + " PCs in request!\n")
                return None
    except Exception as e:
        sys.stderr.write("Exception while parsing server response to forwarded request: " + str(e) + "\n")
        return None
    
    return responseSymbols

def process(input_file, output_file):
    fin = open(input_file, "r")
    fout = gzip.open(output_file, "wb")
    for line in fin:
        splits = line.split("\t");
        try:
            json_dict = json.loads(splits[1])
            if len(json_dict["chromeHangs"]) > 0:
                print "len(chromeHangs) = %d" % (len(json_dict["chromeHangs"]))
                symbol_stacks = symbolicate(json_dict["chromeHangs"])
                del json_dict["chromeHangs"]
                del json_dict["histograms"]
                if symbol_stacks:
                    for stack in symbol_stacks:
                        fout.write(splits[0].rstrip())
                        fout.write("\t")
                        fout.write(json.dumps(json_dict))
                        fout.write("\n----- BEGIN SYMBOL STACK -----\n")
                        fout.write("\n".join(stack))
                        fout.write("\n----- END SYMBOL STACK -----\n")
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
