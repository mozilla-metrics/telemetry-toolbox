/**
 * Copyright 2012 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.telemetry.pig.eval;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;

public abstract class HistogramTupleBase extends EvalFunc<DataBag> {

    protected static BagFactory bagFactory = BagFactory.getInstance();
    protected static TupleFactory tupleFactory = TupleFactory.getInstance();

    protected static final String SIMPLE_MEASURES_PREFIX = "SIMPLE_MEASURES_";
    
    protected static final int DAY_IN_MINUTES = 1440;
    
    // output tuple fields
    protected static final int OUTPUT_TUPLE_SIZE = 9;
    protected static final int HIST_NAME_IDX = 0;
    protected static final int HIST_VALUE_IDX = 1;
    protected static final int VALUE_COUNT_IDX = 2;
    protected static final int SUM_IDX = 3;
    protected static final int BUCKET_COUNT_IDX = 4;
    protected static final int MIN_RANGE_IDX = 5;
    protected static final int MAX_RANGE_IDX = 6;
    protected static final int HIST_TYPE_IDX = 7; 
    protected static final int HIST_IS_VALID = 8;
    
    protected long[] uptimeBuckets; 
    
    public HistogramTupleBase() {
        // setup the uptime buckets
        uptimeBuckets = new long[(8 + ((DAY_IN_MINUTES-120) / 60))];
        uptimeBuckets[0] = 0;
        uptimeBuckets[1] = 5;
        uptimeBuckets[2] = 15;
        uptimeBuckets[3] = 30;
        uptimeBuckets[4] = 60;
        uptimeBuckets[5] = 90;
        uptimeBuckets[6] = 120;
        int i=7;
        for (int t=180; t <= DAY_IN_MINUTES; t += 60) {
            uptimeBuckets[i] = t;
            i++;
        }
        uptimeBuckets[uptimeBuckets.length-1] = 2880;
    }
    
    /**
     * @param uptime
     * @return
     */
    protected String bucketUptime(int uptime) {
        String bucket = "-1";
        for (int i=0; i < uptimeBuckets.length; i++) {
            if ((i+1) < uptimeBuckets.length) {
                if (uptime >= uptimeBuckets[i] && uptime < uptimeBuckets[i+1]) {
                    bucket = String.valueOf(uptimeBuckets[i]);
                    break;
                }   
            } else {
                bucket = String.valueOf(uptimeBuckets[i]);
            }
        }
        
        return bucket;
    }
    
    /**
     * @param t
     * @param minBucket
     * @param maxBucket
     * @param bucketInterval
     * @param defaultBucket
     * @return
     */
    protected String bucketGenericTime(long t, long minBucket, long maxBucket, long bucketInterval, String defaultBucket) {
        if (bucketInterval < 1) {
            throw new IllegalArgumentException("Interval must be greater than or equal to 1");
        }
        
        String bucket = defaultBucket;
        if (t < 0) {
            bucket = "-1";
        } else {
            for (long l=minBucket; l <= maxBucket; l+=bucketInterval) {
                if (t >= l && t < (l+bucketInterval)) {
                    bucket = String.valueOf(l);
                    break;
                }
            }
        }
        
        return bucket;
    }

}
