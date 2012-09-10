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
    
    public static enum STATS { INVALID_ROW_SIZE };
    
    // tuple field indices
    private static final int VALID_ROW_SIZE = 19;
    private static final int DATE_IDX = 0;
    private static final int REASON_IDX = 1;
    private static final int PRODUCT_IDX = 2;
    private static final int PRODUCT_VERSION_IDX = 3;
    private static final int CHANNEL_IDX = 4;
    private static final int ARCH_IDX = 5;
    private static final int OS_IDX = 6;
    private static final int OS_VERSION_IDX = 7;
    private static final int APP_BUILD_ID_IDX = 8;
    private static final int PLAT_BUILD_ID_IDX = 9;
    private static final int HIST_NAME_IDX = 10;
    private static final int HIST_VALUE_IDX = 11;
    private static final int BUCKET_COUNT_IDX = 12;
    private static final int MIN_RANGE_IDX = 13;
    private static final int MAX_RANGE_IDX = 14;
    private static final int HIST_TYPE_IDX = 15;
    private static final int VALUE_SUM_COUNT_IDX = 16;
    private static final int HIST_NAME_SUM_IDX = 17;
    private static final int HIST_NAME_DOC_COUNT_IDX = 18;
    
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        Tuple groupTuple = (Tuple)input.get(0);
        TelemetryDataAggregate tda = new TelemetryDataAggregate();
        tda.setDate((String)groupTuple.get(DATE_IDX));
        Info info = new Info();
        info.setReason((String)groupTuple.get(REASON_IDX));
        info.setAppName((String)groupTuple.get(PRODUCT_IDX));
        info.setAppVersion((String)groupTuple.get(PRODUCT_VERSION_IDX));
        info.setAppUpdateChannel((String)groupTuple.get(CHANNEL_IDX));
        info.setArch((String)groupTuple.get(ARCH_IDX));
        info.setOS((String)groupTuple.get(OS_IDX));
        info.setVersion((String)groupTuple.get(OS_VERSION_IDX));
        info.setAppBuildId((String)groupTuple.get(APP_BUILD_ID_IDX));
        info.setPlatformBuildId((String)groupTuple.get(PLAT_BUILD_ID_IDX));
        tda.setInfo(info);
        DataBag db = (DataBag)input.get(1);
        for (Iterator<Tuple> iter = db.iterator(); iter.hasNext(); ) {
            Tuple t = iter.next();
            if (t.size() != VALID_ROW_SIZE) {
                pigLogger.warn(this, "Invalid row size: " + t.size(), STATS.INVALID_ROW_SIZE);
                continue;
            }
            String histName = (String)t.get(HIST_NAME_IDX);
            tda.addOrPutHistogramValue(histName, String.valueOf(t.get(HIST_VALUE_IDX)), ((Number)t.get(VALUE_SUM_COUNT_IDX)).longValue());
            tda.setHistogramSum(histName, ((Number)t.get(HIST_NAME_SUM_IDX)).longValue());
            tda.setHistogramCount(histName, ((Number)t.get(HIST_NAME_DOC_COUNT_IDX)).intValue());
            tda.setHistogramBucketCount(histName, ((Number)t.get(BUCKET_COUNT_IDX)).intValue());
            tda.setHistogramRange(histName, ((Number)t.get(MIN_RANGE_IDX)).longValue(), ((Number)t.get(MAX_RANGE_IDX)).longValue());
            tda.setHistogramType(histName, ((Number)t.get(HIST_TYPE_IDX)).intValue());
        }

        return jsonMapper.writeValueAsString(tda);
    }

}
