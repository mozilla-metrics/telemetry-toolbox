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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.mozilla.telemetry.pig.eval.HistogramValueTuples.ERRORS;

public class HistogramTuples extends EvalFunc<DataBag> {
    
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private static final int OUTPUT_TUPLE_SIZE = 7;
    private static final int HIST_NAME_IDX = 0;
    private static final int SUM_IDX = 1;
    private static final int BUCKET_COUNT_IDX = 2;
    private static final int MIN_RANGE_IDX = 3;
    private static final int MAX_RANGE_IDX = 4;
    private static final int HIST_TYPE_IDX = 5;
    private static final int HIST_IS_VALID = 6;
    
    private static final String SIMPLE_MEASURES_PREFIX = "SIMPLE_MEASURES_";
    private static final int DAY_IN_MINUTES = 1440;
    
    private long[] uptimeBuckets; 
    
    public HistogramTuples() {
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
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        try {
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
                        int isValid = 1;
                        if (hv.containsKey("valid")) {
                            isValid = Boolean.parseBoolean((String)hv.get("valid")) ? 1 : 0;
                        }
                        
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
    
                        Tuple t = tupleFactory.newTuple(OUTPUT_TUPLE_SIZE);
                        t.set(HIST_NAME_IDX, hist.getKey());
                        t.set(SUM_IDX, sum);
                        t.set(BUCKET_COUNT_IDX, bucketCount);
                        t.set(MIN_RANGE_IDX, minRange);
                        t.set(MAX_RANGE_IDX, maxRange);
                        t.set(HIST_TYPE_IDX, histogramType);
                        t.set(HIST_IS_VALID, isValid);
                        output.add(t);
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
                                t.set(SUM_IDX, booleanNum);
                                t.set(BUCKET_COUNT_IDX, 2);
                                t.set(MIN_RANGE_IDX, 0);
                                t.set(MAX_RANGE_IDX, 1);
                                t.set(HIST_TYPE_IDX, 2);
                                t.set(HIST_IS_VALID, 1);
                                output.add(t);
                            }
                        }
                    } else if (vo != null) {
                        long timeValue = ((Number)vo).longValue();
                        Tuple t = tupleFactory.newTuple(OUTPUT_TUPLE_SIZE);
                        t.set(HIST_NAME_IDX, measureKey);
                        
                        int bucketCount = 0;
                        int minRange = 0, maxRange = 0;
                        int histogramType = 0;
                        
                        if ("uptime".equals(measure.getKey())) {
                            bucketCount = uptimeBuckets.length;
                            minRange = (int)uptimeBuckets[0];
                            maxRange = (int)uptimeBuckets[uptimeBuckets.length-1];
                        } else if ("startupInterrupted".equals(measure.getKey())) {
                            bucketCount = 2;
                            maxRange = 1;
                            histogramType = 2;
                        } else if ("startupCrashDetectionEnd".equals(measure.getKey())) {
                            bucketCount = 30;
                            minRange = 30000;
                            maxRange = 90000;
                            histogramType = 1;
                        } else if ("savedPings".equals(measure.getKey())) {
                            bucketCount = 51;
                            maxRange = 50;
                        } else {
                            bucketCount = 31;
                            maxRange = 30001;
                            histogramType = 1;
                        }
                        
                        t.set(SUM_IDX, timeValue);
                        t.set(BUCKET_COUNT_IDX, bucketCount);
                        t.set(MIN_RANGE_IDX, minRange);
                        t.set(MAX_RANGE_IDX, maxRange);
                        t.set(HIST_TYPE_IDX, histogramType);
                        t.set(HIST_IS_VALID, 1);
                        output.add(t);
                    }
                }
            }
            return output;
        } catch (Exception e) {
            warn("Exception while processing histogram values", ERRORS.GENERIC_ERROR);
        }
        
        return null;
    }
}
