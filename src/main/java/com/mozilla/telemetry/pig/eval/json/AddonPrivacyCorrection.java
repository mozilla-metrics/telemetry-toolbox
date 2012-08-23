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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.mozilla.util.Pair;

public class AddonPrivacyCorrection extends EvalFunc<String> {
    
    private static final Logger LOG = Logger.getLogger(AddonPrivacyCorrection.class);
    
    public static enum ERRORS {
        JSONParseError, JSONMappingError, EOFError, GenericError
    };
    
    private static final String SLOW_SQL = "slowSQL";
    private static final String OTHER_THREADS = "otherThreads";
    
    private final ObjectMapper jsonMapper;
    private final List<Pattern> patterns;
    
    public AddonPrivacyCorrection() {
        jsonMapper = new ObjectMapper();
        
        patterns = new ArrayList<Pattern>();
        Pattern singleQuoteLiteral = Pattern.compile("'[^'\\\\]*(?:\\\\.[^'\\\\]*)*'");
        patterns.add(singleQuoteLiteral);
        Pattern doubleQuoteLiteral = Pattern.compile("'[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*\"");
        patterns.add(doubleQuoteLiteral);
    }
    
    private Pair<Boolean,String> process(String input) {
        Pair<Boolean,String> result = new Pair<Boolean,String>(false, input);
        for (Pattern p : patterns) {
            Matcher m = p.matcher(result.getSecond());
            if (m.find()) {
                LOG.info("Original: " + input);
                result.setFirst(true);
                result.setSecond(m.replaceAll(":private"));
                LOG.info("Modified: " + result.getSecond());
            }
        }
        return result;
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
                            Pair<Boolean,String> result = process(otherThread.getKey());
                            if (result.getFirst()) {
                                modified = true; 
                                if (newOtherThreadsMap.containsKey(result.getSecond())) {
                                    List<Object> existingList =  (List<Object>)newOtherThreadsMap.get(result.getSecond());
                                    List<Object> valList = (List<Object>)otherThread.getValue();
                                    valList.set(0, ((Number)existingList.get(0)).intValue() + ((Number)valList.get(0)).intValue());
                                    valList.set(1, ((Number)existingList.get(1)).intValue() + ((Number)valList.get(1)).intValue());
                                    newOtherThreadsMap.put(result.getSecond(), valList);
                                } else {
                                    newOtherThreadsMap.put(result.getSecond(), otherThread.getValue());
                                }
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