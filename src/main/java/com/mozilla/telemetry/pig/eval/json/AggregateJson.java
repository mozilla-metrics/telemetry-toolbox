/*
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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.telemetry.pig.eval.json;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.map.ObjectMapper;

import com.mozilla.telemetry.elasticsearch.TelemetryDataAggregate;
import com.mozilla.telemetry.elasticsearch.TelemetryDataAggregate.Info;

public class AggregateJson extends EvalFunc<String> {

    private static final ObjectMapper jsonMapper = new ObjectMapper();
    
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        Tuple groupTuple = (Tuple)input.get(0);
        TelemetryDataAggregate tda = new TelemetryDataAggregate();
        tda.setDate((String)groupTuple.get(0));
        Info info = new Info();
        info.setAppName((String)groupTuple.get(1));
        info.setAppVersion((String)groupTuple.get(2));
        info.setAppUpdateChannel((String)groupTuple.get(3));
        info.setArch((String)groupTuple.get(4));
        info.setOS((String)groupTuple.get(5));
        info.setVersion((String)groupTuple.get(6));
        info.setAppBuildId((String)groupTuple.get(7));
        info.setPlatformBuildId((String)groupTuple.get(8));
        tda.setInfo(info);
        DataBag db = (DataBag)input.get(1);
        for (Iterator<Tuple> iter = db.iterator(); iter.hasNext(); ) {
            Tuple t = iter.next();
            String histName = (String)t.get(9);
            tda.addOrPutHistogramValue(histName, String.valueOf(t.get(10)), ((Number)t.get(15)).longValue());
            tda.setHistogramCount(histName, ((Number)t.get(18)).intValue());
            tda.setHistogramSum(histName, ((Number)t.get(16)).longValue());
            tda.setHistogramBucketCount(histName, ((Number)t.get(11)).intValue());
            tda.setHistogramRange(histName, ((Number)t.get(12)).intValue(), ((Number)t.get(13)).intValue());
            tda.setHistogramType(histName, ((Number)t.get(14)).intValue());
        }

        return jsonMapper.writeValueAsString(tda);
    }

}
