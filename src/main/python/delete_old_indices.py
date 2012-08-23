#!/usr/bin/env python
# encoding: utf-8
"""
show_indices.py
"""

import sys
import os
import pyes
from datetime import datetime

ES_HOSTNAME = 'localhost'

def main():
    conn = pyes.ES([ES_HOSTNAME], timeout=30.0)
    indices_dict = conn.get_indices()
    
    for k in sorted(indices_dict.iterkeys()):
        if k.startswith('telemetry_agg'):
            d = datetime.strptime(k.replace('telemetry_agg_',''), '%Y%m%d')
            now = datetime.now()
            td = now - d
            if td.days > 1:
                print str(k) + " => " + str(indices_dict[k])
                conn.delete_index_if_exists(k)

if __name__ == '__main__':
    main()

