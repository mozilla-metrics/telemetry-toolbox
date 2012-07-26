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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.telemetry.pig.eval.json;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class AddonPrivacyCorrection extends EvalFunc<String> {
    
    public static enum ERRORS {
        JSONParseError, JSONMappingError, EOFError, GenericError
    };
    
    private static final String SLOW_SQL = "slowSQL";
    private static final String OTHER_THREADS = "otherThreads";
    
    private final ObjectMapper jsonMapper;
    private final Pattern sqlLikePattern;
    
    public AddonPrivacyCorrection() {
        jsonMapper = new ObjectMapper();
        sqlLikePattern = Pattern.compile("WHERE\\s+([a-zA-Z0-9_]+)\\s+LIKE\\s+('[^']+')");
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        boolean modified = false;
        String json = (String)input.get(0);
        try {
            if (json != null) {
                Map<String,Object> values = jsonMapper.readValue(json, 
                                                                 new TypeReference<Map<String,Object>>() {});
                if (values.containsKey(SLOW_SQL)) {
                    Map<String,Object> slowSQLMap = (Map<String, Object>)values.get(SLOW_SQL);
                    if (slowSQLMap.containsKey(OTHER_THREADS)) {
                        Map<String,Object> otherThreadsMap = (Map<String, Object>)slowSQLMap.get(OTHER_THREADS);
                        Map<String,Object> newOtherThreadsMap = new HashMap<String,Object>();
                        for (Map.Entry<String, Object> otherThread : otherThreadsMap.entrySet()) {
                            Matcher m = sqlLikePattern.matcher(otherThread.getKey());
                            if (m.find()) {
                                String col = m.group(1);
                                String pred = m.group(2);
                                String newKey = otherThread.getKey().replaceAll(Pattern.quote(pred), ":" + col);
                                if (newOtherThreadsMap.containsKey(newKey)) {
                                    List<Object> existingList =  (List<Object>)newOtherThreadsMap.get(newKey);
                                    List<Object> valList = (List<Object>)otherThread.getValue();
                                    valList.set(0, ((Number)existingList.get(0)).intValue() + ((Number)valList.get(0)).intValue());
                                    valList.set(1, ((Number)existingList.get(1)).intValue() + ((Number)valList.get(1)).intValue());
                                    newOtherThreadsMap.put(newKey, valList);
                                } else {
                                    newOtherThreadsMap.put(newKey, otherThread.getValue());
                                }
                                modified = true;
                            } else {
                                // keep calm and carry on
                                newOtherThreadsMap.put(otherThread.getKey(), otherThread.getValue());
                            }
                        }
                        
                        if (modified) {
                            slowSQLMap.put(OTHER_THREADS, newOtherThreadsMap);
                            values.put(SLOW_SQL, slowSQLMap);
                            json = jsonMapper.writeValueAsString(values);
                        }
                    }
                }
            }
        } catch (JsonParseException e) {
            warn("JSON Parse Error: " + e.getMessage(), ERRORS.JSONParseError);
        } catch (JsonMappingException e) {
            warn("JSON Mapping Error: " + e.getMessage(), ERRORS.JSONMappingError);
        } catch (EOFException e) {
            warn("Hit EOF unexpectedly", ERRORS.EOFError);
        } catch (Exception e) {
            warn("Generic error during JSON mapping", ERRORS.GenericError);
        }
        
        return (modified ? json : null);
    }

}