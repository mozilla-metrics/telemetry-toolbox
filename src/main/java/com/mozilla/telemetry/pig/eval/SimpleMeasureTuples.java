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
import java.util.Map;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class SimpleMeasureTuples extends HistogramTupleBase {
        
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        DataBag output = bagFactory.newDefaultBag();
        Map<String,Object> m = (Map<String,Object>)input.get(0);
        if (m != null) {
            for (Map.Entry<String, Object> measure : m.entrySet()) {
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
                        t.set(HIST_VALUE_IDX, bucketUptime((int)timeValue));
                        bucketCount = uptimeBuckets.length;
                        minRange = (int)uptimeBuckets[0];
                        maxRange = (int)uptimeBuckets[uptimeBuckets.length-1];
                    } else if ("startupInterrupted".equals(measure.getKey())) {
                        t.set(HIST_VALUE_IDX, String.valueOf(timeValue));
                        bucketCount = 2;
                        maxRange = 1;
                        histogramType = 2;
                    } else if ("startupCrashDetectionEnd".equals(measure.getKey())) {
                        bucketCount = 30;
                        minRange = 30000;
                        maxRange = 90000;
                        histogramType = 1;
                        t.set(HIST_VALUE_IDX, bucketGenericTime(timeValue, minRange, maxRange, ((maxRange-minRange)/bucketCount), "0"));
                    } else if ("savedPings".equals(measure.getKey())) {
                        bucketCount = 51;
                        maxRange = 50;
                        t.set(HIST_VALUE_IDX, bucketGenericTime(timeValue, minRange, maxRange, 1, "0"));
                    } else {
                        bucketCount = 31;
                        maxRange = 30001;
                        histogramType = 1;
                        t.set(HIST_VALUE_IDX, bucketGenericTime(timeValue, minRange, maxRange, 1000, "0"));
                    }
                    t.set(VALUE_COUNT_IDX, 1.0d);
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
    }

}