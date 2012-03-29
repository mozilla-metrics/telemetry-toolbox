/**
 * Copyright 2011 Mozilla Foundation
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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class HistogramValueTuples extends EvalFunc<DataBag> {

    public static enum ERRORS { BAD_HISTOGRAM_TUPLE, NON_NUMERIC_HIST_VALUE };
    
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private static final int OUTPUT_TUPLE_SIZE = 8;
    private static final int HIST_NAME_IDX = 0;
    private static final int HIST_VALUE_IDX = 1;
    private static final int VALUE_COUNT_IDX = 2;
    private static final int SUM_IDX = 3;
    private static final int BUCKET_COUNT_IDX = 4;
    private static final int MIN_RANGE_IDX = 5;
    private static final int MAX_RANGE_IDX = 6;
    private static final int HIST_TYPE_IDX = 7;
    
    private static final String SIMPLE_MEASURES_PREFIX = "SIMPLE_MEASURES_";
    private static final int DAY_IN_MINUTES = 1440;

    private long[] uptimeBuckets; 
            
    public HistogramValueTuples() {
        // setup the uptime buckets
        uptimeBuckets = new long[(9 + ((DAY_IN_MINUTES-180) / 60))];
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
    private String bucketUptime(int uptime) {
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
     * @return
     */
    private String bucketGenericTime(long t) {
        String bucket = "0";
        if (t < 0) {
            bucket = "-1";
        } else if (t >= 0 && t < 30000) {
            bucket = String.valueOf((long)Math.round((double)t / 1000.0d) * 1000);
        } else {
            bucket = "30001";
        }
        
        return bucket;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        DataBag output = bagFactory.newDefaultBag();
        Map<String,Map<String,Object>> m = (Map<String,Map<String,Object>>)input.get(0);
        if (m != null) {
            for (Map.Entry<String, Map<String,Object>> hist : m.entrySet()) {
                Map<String,Object> hv = hist.getValue();
                if (hv != null) {
                    Map<String,Object> values = (Map<String,Object>)hv.get("values");
                    // If any of these cases are true just flag the entry as bad and continue
                    if (values == null || !hv.containsKey("sum") || !hv.containsKey("bucket_count") ||
                        !hv.containsKey("range") || !hv.containsKey("histogram_type") ||
                            hv.get("sum") == null || hv.get("bucket_count") == null || 
                            hv.get("range") == null || hv.get("histogram_type") == null) {
                            warn("Encountered bad histogram tuple", ERRORS.BAD_HISTOGRAM_TUPLE);
                            continue;
                    }

                    long sum = ((Number)hv.get("sum")).longValue();
                    int bucketCount = ((Number)hv.get("bucket_count")).intValue();
                    int histogramType = ((Number)hv.get("histogram_type")).intValue();
                    
                    int minRange = 0, maxRange = 0;
                    DataBag rangeBag = (DataBag)hv.get("range");
                    Iterator<Tuple> rangeIter = rangeBag.iterator();
                    Tuple rangeTuple = rangeIter.next();
                    if (rangeTuple.size() >= 2) {
                        if (rangeTuple.get(0) instanceof Number) {
                            minRange = ((Number)rangeTuple.get(0)).intValue();
                        }
                        if (rangeTuple.get(1) instanceof Number) {
                            maxRange = ((Number)rangeTuple.get(1)).intValue();
                        }
                    }

                    for (Map.Entry<String, Object> v : values.entrySet()) {
                        long histValue = -1;
                        try {
                            histValue = Long.parseLong(v.getKey());
                        } catch (NumberFormatException e) {
                            pigLogger.warn(this, "Non-numeric histogram value incountered", ERRORS.NON_NUMERIC_HIST_VALUE);
                            continue;
                        }
                        Tuple t = tupleFactory.newTuple(OUTPUT_TUPLE_SIZE);
                        t.set(HIST_NAME_IDX, hist.getKey());
                        t.set(HIST_VALUE_IDX, histValue);
                        t.set(VALUE_COUNT_IDX, ((Number)v.getValue()).doubleValue());
                        t.set(SUM_IDX, sum);
                        t.set(BUCKET_COUNT_IDX, bucketCount);
                        t.set(MIN_RANGE_IDX, minRange);
                        t.set(MAX_RANGE_IDX, maxRange);
                        t.set(HIST_TYPE_IDX, histogramType);
                        output.add(t);
                    }
                }
            }
        }
        
        Map<String,Object> smMap = (Map<String,Object>)input.get(1);
        if (smMap != null) {
            for (Map.Entry<String, Object> measure : smMap.entrySet()) {
                String measureKey = SIMPLE_MEASURES_PREFIX + measure.getKey().toUpperCase();
                Object vo = measure.getValue();
                if (vo != null && vo instanceof Map) {
                    for (Map.Entry<String, Object> js : ((Map<String,Object>)vo).entrySet()) {
                        Object jso = js.getValue();
                        if (jso != null) {
                            long booleanNum = ((Number)jso).longValue();
                            Tuple t = tupleFactory.newTuple(OUTPUT_TUPLE_SIZE);
                            t.set(HIST_NAME_IDX, measureKey + "_" + js.getKey().toUpperCase());
                            t.set(HIST_VALUE_IDX, booleanNum);
                            t.set(VALUE_COUNT_IDX, 1.0d);
                            t.set(SUM_IDX, booleanNum);
                            t.set(BUCKET_COUNT_IDX, 2);
                            t.set(MIN_RANGE_IDX, 0);
                            t.set(MAX_RANGE_IDX, 1);
                            t.set(HIST_TYPE_IDX, 2);
                            output.add(t);
                        }
                    }
                } else if (vo != null) {
                    long timeValue = ((Number)vo).longValue();
                    Tuple t = tupleFactory.newTuple(8);
                    t.set(HIST_NAME_IDX, measureKey);
                    
                    int bucketCount = 0;
                    int minRange = 0, maxRange = 0;
                    int histogramType = 0;
                    if ("uptime".equals(measure.getKey())) {
                        t.set(HIST_VALUE_IDX, bucketUptime((int)timeValue));
                        bucketCount = 3;
                        maxRange = 1441;
                    } else if ("startupInterrupted".equals(measure.getKey())) {
                        t.set(HIST_VALUE_IDX, String.valueOf(timeValue));
                        bucketCount = 2;
                        maxRange = 1;
                        histogramType = 2;
                    } else {
                        t.set(HIST_VALUE_IDX, bucketGenericTime(timeValue));
                        bucketCount = 31;
                        maxRange = 30001;
                        histogramType = 1;
                    }
                    
                    t.set(VALUE_COUNT_IDX, 1.0d);
                    t.set(SUM_IDX, timeValue);
                    t.set(BUCKET_COUNT_IDX, bucketCount);
                    t.set(MIN_RANGE_IDX, minRange);
                    t.set(MAX_RANGE_IDX, maxRange);
                    t.set(HIST_TYPE_IDX, histogramType);
                    output.add(t);
                }
            }
        }
        
        return output;
    }
}
