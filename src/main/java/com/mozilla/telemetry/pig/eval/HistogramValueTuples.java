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

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
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
                    if (values != null) {
                        long sum = 0;
                        if (hv.containsKey("sum")) {
                            sum = ((Number)hv.get("sum")).longValue();
                        }
                        int bucketCount = 0;
                        if (hv.containsKey("bucket_count")) {
                            bucketCount = ((Number)hv.get("bucket_count")).intValue();
                        }
                        int minRange = 0, maxRange = 0;
                        if (hv.containsKey("range")) {
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
                        }
                        int histogramType = 0;
                        if (hv.containsKey("histogram_type")) {
                            histogramType = ((Number)hv.get("histogram_type")).intValue();
                        }
                        for (Map.Entry<String, Object> v : values.entrySet()) {
                            Tuple t = tupleFactory.newTuple(8);
                            t.set(0, hist.getKey());
                            t.set(1, v.getKey());
                            t.set(2, ((Number)v.getValue()).doubleValue());
                            t.set(3, sum);
                            t.set(4, bucketCount);
                            t.set(5, minRange);
                            t.set(6, maxRange);
                            t.set(7, histogramType);
                            output.add(t);
                        }
                    }
                }
            }
        }
        
        return output;
    }

}
