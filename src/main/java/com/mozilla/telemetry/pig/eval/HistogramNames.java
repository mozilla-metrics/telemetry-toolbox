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
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class HistogramNames  extends EvalFunc<DataBag> {
    
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private static final String SIMPLE_MEASURES_PREFIX = "SIMPLE_MEASURES_";
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        DataBag output = bagFactory.newDefaultBag();
        Map<String,Map<String,Object>> m = (Map<String,Map<String,Object>>)input.get(0);
        if (m != null) {
            for (String histName : m.keySet()) {
                output.add(tupleFactory.newTuple(histName));
            }
        }
        
        Map<String,Object> smMap = (Map<String,Object>)input.get(1);
        if (smMap != null) {
            for (String measureName : smMap.keySet()) {
                String measureKey = SIMPLE_MEASURES_PREFIX + measureName.toUpperCase();
                output.add(tupleFactory.newTuple(measureKey));
            }
        }
        
        return output;
    }
    
}
