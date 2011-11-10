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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class SimpleMeasureTuples extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();

    private static final String SIMPLE_MEASURES_PREFIX = "SIMPLE_MEASURES_";
    
    private static final long DAY_IN_MINUTES = 1440;
    
    /**
     * @param uptime
     * @return
     */
    private String bucketUptime(long uptime) {
        String bucket = "0";
        if (uptime < 0) {
            bucket = "-1";
        } else if (uptime >= 0 && uptime <= DAY_IN_MINUTES) {
            bucket = "720";
        } else if (uptime > DAY_IN_MINUTES) {
            bucket = "1441";
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
        Map<String,Object> m = (Map<String,Object>)input.get(0);
        if (m != null) {
            for (Map.Entry<String, Object> measure : m.entrySet()) {
                String measureKey = SIMPLE_MEASURES_PREFIX + measure.getKey().toUpperCase();
                Object vo = measure.getValue();
                if (vo != null && vo instanceof Map) {
                    for (Map.Entry<String, Object> js : ((Map<String,Object>)vo).entrySet()) {
                        long booleanNum = ((Number)js.getValue()).longValue();
                        Tuple t = tupleFactory.newTuple(8);
                        t.set(0, measureKey + "_" + js.getKey().toUpperCase());
                        t.set(1, booleanNum);
                        t.set(2, 1.0d);
                        t.set(3, booleanNum);
                        t.set(4, 2);
                        t.set(5, 0);
                        t.set(6, 1);
                        t.set(7, 2);
                        output.add(t);
                    }
                } else if (vo != null) {
                    long timeValue = ((Number)vo).longValue();
                    Tuple t = tupleFactory.newTuple(8);
                    t.set(0, measureKey);
                    
                    int bucketCount = 0;
                    int minRange = 0, maxRange = 0;
                    int histogramType = 0;
                    if ("uptime".equals(measure.getKey())) {
                        t.set(1, bucketUptime(timeValue));
                        bucketCount = 3;
                        maxRange = 1441;
                    } else if ("startupInterrupted".equals(measure.getKey())) {
                        t.set(1, String.valueOf(timeValue));
                        bucketCount = 2;
                        maxRange = 1;
                        histogramType = 2;
                    } else {
                        t.set(1, bucketGenericTime(timeValue));
                        bucketCount = 31;
                        maxRange = 30001;
                        histogramType = 1;
                    }
                    
                    t.set(2, 1.0d);
                    t.set(3, timeValue);
                    t.set(4, bucketCount);
                    t.set(5, minRange);
                    t.set(6, maxRange);
                    t.set(7, histogramType);
                    output.add(t);
                }
            }
        }
        
        return output;
    }

}