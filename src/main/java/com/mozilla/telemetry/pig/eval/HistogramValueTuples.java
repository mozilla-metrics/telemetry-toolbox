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

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class HistogramValueTuples extends HistogramTupleBase {

    public static enum ERRORS { BAD_HISTOGRAM_TUPLE, NON_NUMERIC_HIST_VALUE, GENERIC_ERROR };
    
    private SimpleMeasureTuples smt;
    
    public HistogramValueTuples() {
        super();
        smt = new SimpleMeasureTuples();
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
                            t.set(HIST_IS_VALID, isValid);
                            output.add(t);
                        }
                    }
                }
            }

            Map<String,Object> smMap = (Map<String,Object>)input.get(1);
            if (smMap != null) {  
                DataBag resultsBag = smt.exec(tupleFactory.newTuple(smMap));
                output.addAll(resultsBag);
            }
            
            return output;
        } catch (Exception e) {
            warn("Exception while processing histogram values", ERRORS.GENERIC_ERROR);
        }
        
        return null;
    }
}
