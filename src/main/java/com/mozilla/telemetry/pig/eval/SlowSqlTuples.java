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
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class SlowSqlTuples  extends EvalFunc<DataBag> {

    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private Pattern spacePattern = Pattern.compile("\\s+");
    private Pattern reservedCharacters = Pattern.compile("\u0001");
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        DataBag output = bagFactory.newDefaultBag();
        Map<String,Map<String,Object>> m = (Map<String,Map<String,Object>>)input.get(0);
        if (m != null) {
            for (Map.Entry<String, Map<String,Object>> entry : m.entrySet()) {
                DefaultDataBag dbag = (DefaultDataBag)entry.getValue();
                
                Tuple t = tupleFactory.newTuple();
                String modified = reservedCharacters.matcher(entry.getKey()).replaceAll("\\\\u0001");
                t.append(spacePattern.matcher(modified).replaceAll(" ").trim());
                Iterator<Tuple> bagIter = dbag.iterator();
                while (bagIter.hasNext()) {
                    Tuple inner = bagIter.next();
                    for (int i=0; i < inner.size(); i++) {
                        t.append(((Number)inner.get(i)).longValue());
                    }
                }
                output.add(t);
            }
        }
        
        return output;
    }

}