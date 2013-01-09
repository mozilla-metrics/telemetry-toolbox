#!/usr/bin/env python

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
A trivial script to convert a file from <profile>/saved-telemetry-pings into
the format that is used by symbolicate.py.
"""

import json
import sys

def main():
    fname = sys.argv[1]
    data = json.load(file(fname))
    print 'dummy\t%s' % data["payload"]

if __name__ == "__main__":
	sys.exit(main())
