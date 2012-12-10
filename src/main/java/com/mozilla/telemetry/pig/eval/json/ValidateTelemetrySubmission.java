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

import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.ParseException;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Properties;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;


import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;

//import org.apache.pig.tools.counters.PigCounterHelper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.lang.StringUtils;

import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.type.TypeReference;

import com.twitter.elephantbird.pig.util.PigCounterHelper;

import com.mozilla.telemetry.constants.TelemetryConstants;

public class ValidateTelemetrySubmission extends EvalFunc<String> {
    Map <String, Object> specValues = null;
    int count = 0;
    final String lookupFilename;
    static final Logger LOG = Logger.getLogger(ValidateTelemetrySubmission.class);
    PigCounterHelper pigCounterHelper = new PigCounterHelper();
    TupleFactory tupleFactory = TupleFactory.getInstance();
    enum ReportStats {VALID_HISTOGRAM,INVALID_HISTOGRAM, INVALID_JSON_STRUCTURE,INVALID_SUBMISSIONS,KNOWN_HISTOGRAMS,
                             UNKNOWN_HISTOGRAMS, META_DATA_INVALID, UNDEFINED_HISTOGRAMS, MISSING_JSON_REFERENCE,
                             SUBMISSIONS_EVALUATED, SUBMISSIONS_SKIPPED, MISSING_JSON_VALUES_FIELD,JSON_INVALID_VALUES_FIELD,
                             INVALID_HISTOGRAM_BUCKET_VALUE,INVALID_HISTOGRAM_BUCKET_COUNT,INVALID_HISTOGRAM_MAX,
                             INVALID_HISTOGRAM_MIN,INVALID_HISTOGRAM_TYPE,NO_HISTOGRAM_BUCKET_VALUES};
    
    public ValidateTelemetrySubmission(String filename) {
        lookupFilename = filename;
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if(specValues == null) {
            readLookupFile();
        }
        DataByteArray key = (DataByteArray)input.get(0);
        String json = (String)input.get(1);
        if(json == null) {
            LOG.info("json is null "+key.toString());
            pigCounterHelper.incrCounter(ReportStats.META_DATA_INVALID,1L);
        } else {
            String newJson = validateTelemetryJson(json);
            if(newJson != null) {
                return newJson;
            } 
        }
        return null;
    }

    protected FSDataInputStream getHDFSFile(String fileName) throws IOException {
        FileSystem fs = FileSystem.get(UDFContext.getUDFContext().getJobConf());
        FSDataInputStream fi = fs.open(new Path(fileName));
        return fi;
    }
    
    protected void readLookupFile() {
        try {
            Properties telemetry_spec_properties = new Properties();
            specValues = new HashMap<String,Object>();
            FSDataInputStream fi = getHDFSFile(lookupFilename);
            telemetry_spec_properties.load(fi);
            for(String key: telemetry_spec_properties.stringPropertyNames()) {
                String spec_file = telemetry_spec_properties.getProperty(key);
                LOG.info(key+" "+spec_file);
                Map<String, Map<String,Object>> referenceJson = readReferenceJson(spec_file);
                specValues.put(key,referenceJson);
            }
        } catch(IOException e) {
            LOG.info("ERROR: failed to process telemetry spec lookup file "+e.getMessage());
        } catch(Exception e) {
            e.printStackTrace();
            LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
        }
    }



    @SuppressWarnings("unchecked")    
    protected Map<String, Map<String,Object>>  readReferenceJson(String filename)  {
        try {
            Map<String, Map<String,Object>> referenceValues = new HashMap<String, Map<String,Object>>();
            ObjectMapper jsonMapper = new ObjectMapper();
            Map<String, Object> referenceJson = new LinkedHashMap<String, Object>();
            FSDataInputStream fi = getHDFSFile(filename);
            referenceJson = jsonMapper.readValue(fi, new TypeReference<Map<String,Object>>() { });
            LinkedHashMap<String, Object> histograms = (LinkedHashMap<String, Object>) referenceJson.get(TelemetryConstants.HISTOGRAMS);

            for(Map.Entry<String, Object> histogram : histograms.entrySet()) {
                Map<String, Object> compKey = new HashMap<String, Object>();
                String jKey = histogram.getKey();
                LinkedHashMap<String, Object> histogram_values = (LinkedHashMap<String, Object>) histogram.getValue();
                compKey.put(TelemetryConstants.NAME, jKey);
                if(histogram_values.containsKey(TelemetryConstants.MIN)) {
                    compKey.put(TelemetryConstants.MIN, histogram_values.get(TelemetryConstants.MIN) + "");
                }
                if(histogram_values.containsKey(TelemetryConstants.MAX)) {
                    compKey.put(TelemetryConstants.MAX,histogram_values.get(TelemetryConstants.MAX) + "");
                }
                if(histogram_values.containsKey(TelemetryConstants.KIND)) {
                    compKey.put(TelemetryConstants.HISTOGRAM_TYPE,histogram_values.get(TelemetryConstants.KIND) + "");
                }
                if(histogram_values.containsKey(TelemetryConstants.BUCKET_COUNT)) {
                    compKey.put(TelemetryConstants.BUCKET_COUNT,histogram_values.get(TelemetryConstants.BUCKET_COUNT) + "");
                }
                if(histogram_values.containsKey(TelemetryConstants.BUCKETS)) {
                    compKey.put(TelemetryConstants.BUCKETS,(List<Integer>) histogram_values.get(TelemetryConstants.BUCKETS));
                }
                
                referenceValues.put(jKey, compKey);
            }
            return referenceValues;

        } catch (IOException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected String getAppVersionFromTelemetryDoc(Map<String,Object> crash) {
        LinkedHashMap<String, Object> infoValue = (LinkedHashMap<String, Object>) crash.get(TelemetryConstants.INFO);
        String appVersion = (String) infoValue.get(TelemetryConstants.APP_VERSION);
        return appVersion;
    }
    
    @SuppressWarnings("unchecked")
    protected boolean checkVersion(String appVersion) {
        boolean appVersionMatch = false;
        for(Map.Entry<String, Object> entry : specValues.entrySet()) {
            if (appVersion.contains(entry.getKey()))
                appVersionMatch = true;
        }
        return  appVersionMatch;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Map<String,Object>> getJsonSpec(String appVersion) {
        for(Map.Entry<String, Object> entry : specValues.entrySet()) {
            if (appVersion.contains(entry.getKey())) {
                return (Map<String, Map<String,Object>>) specValues.get(entry.getKey());
            }
        }
        return (Map<String, Map<String,Object>>) specValues.get("default");
    }
    
    @SuppressWarnings("unchecked")
    protected String validateTelemetryJson(String json) {
        String jsonValue = new String(json);
        ObjectMapper jsonMapper = new ObjectMapper();
        Map<String, Object> crash;
        try {
            crash = jsonMapper.readValue(json, new TypeReference<Map<String,Object>>() { });
            String appVersion = getAppVersionFromTelemetryDoc(crash);
            if(appVersion == null) LOG.info("appVersion is null "+json);
            LinkedHashMap<String,Object> info = (LinkedHashMap<String, Object>) crash.get(TelemetryConstants.INFO);
            if(appVersion  == null) {
                info.put(TelemetryConstants.VALID_FOR_SCHEMA,"false");
                return jsonMapper.writeValueAsString(crash);
            }
            Map<String, Map<String,Object>> referenceValues = getJsonSpec(appVersion);
            if(referenceValues == null) 
                LOG.info("referenceValues is null "+appVersion);
            pigCounterHelper.incrCounter(ReportStats.SUBMISSIONS_EVALUATED,1L);
            Map<String, Object> missingJson = new LinkedHashMap<String, Object>();
            LinkedHashMap<String, Object> histograms = (LinkedHashMap<String, Object>) crash.get(TelemetryConstants.HISTOGRAMS);
            boolean validForSchema = true;
            for(Map.Entry<String, Object> entry : histograms.entrySet()) {
                String jKey = entry.getKey();
                String min = new String();
                String max = new String();
                String histogram_type = new String();
                String bucket_count = new String();
                boolean validHistogram = true;
                Map<String,Object> bucket_values = new LinkedHashMap<String,Object>();
                LinkedHashMap<String, Object> histogram_values = (LinkedHashMap<String, Object>) entry.getValue();
                    
                for(Map.Entry<String, Object> histogram_value : histogram_values.entrySet()) {
                    try {
                        if (StringUtils.equals(histogram_value.getKey(), TelemetryConstants.RANGE)) {
                            List<Integer> range = (List<Integer>) histogram_value.getValue();
                            min = range.get(0)+"";
                            max = range.get(1)+"";
                        } else if (StringUtils.equals(histogram_value.getKey(), TelemetryConstants.HISTOGRAM_TYPE)) {
                            histogram_type = histogram_value.getValue() + "";
                        } else if (StringUtils.equals(histogram_value.getKey(), TelemetryConstants.BUCKET_COUNT)) {
                            bucket_count = histogram_value.getValue() + "";
                        } else if(StringUtils.equals(histogram_value.getKey(), TelemetryConstants.VALUES)) {
                            bucket_values = (LinkedHashMap<String,Object>) histogram_value.getValue();
                        }
                    } catch(Exception e ) {
                        LOG.info("error "+e);
                        LOG.info(histogram_value);
                    }
                }
                        
                if (referenceValues.containsKey(jKey)) {
                    pigCounterHelper.incrCounter(ReportStats.KNOWN_HISTOGRAMS,1L);
                    Map<String,Object> referenceHistograms = referenceValues.get(jKey);
                    String reference_histogram_type = (String)referenceHistograms.get(TelemetryConstants.HISTOGRAM_TYPE);

                    if (!StringUtils.equals(reference_histogram_type,histogram_type)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_TYPE,1L);
                    }
                    if (!StringUtils.equals((String)referenceHistograms.get(TelemetryConstants.MIN),min)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_MIN,1L);
                    }
                    if (!StringUtils.equals((String)referenceHistograms.get(TelemetryConstants.MAX), max)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_MAX,1L);
                    }
                    if (!StringUtils.equals((String)referenceHistograms.get(TelemetryConstants.BUCKET_COUNT),bucket_count)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_BUCKET_COUNT,1L);
                    }

                    if (bucket_values.size() <= 0 ) {
                        pigCounterHelper.incrCounter(ReportStats.NO_HISTOGRAM_BUCKET_VALUES,1L);
                        validHistogram = false;
                    } else {
                        LinkedHashMap<String, Integer> invalid_values = new LinkedHashMap<String, Integer>();
                        List<Integer> reference_bucket_values = (List<Integer>)referenceHistograms.get(TelemetryConstants.BUCKETS);
                            
                        for(Map.Entry<String,Object> bucket_value : bucket_values.entrySet()) {
                            int bucket_key = -1;
                            try {
                                bucket_key = Integer.parseInt(bucket_value.getKey());
                            } catch(Exception e) {
                                bucket_key = -1;
                            }
                            if (!reference_bucket_values.contains(bucket_key)) {
                                invalid_values.put(TelemetryConstants.VALUES, bucket_key);
                            } 
                        }

                        if (invalid_values.size() > 0) {
                            pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_BUCKET_VALUE,1L);
                            validHistogram = false;
                        }
                        invalid_values = null;
                    }

                    if(validHistogram) {
                        histogram_values.put(TelemetryConstants.VALID,"true");
                        pigCounterHelper.incrCounter(ReportStats.VALID_HISTOGRAM,1L);
                    } else {
                        validForSchema = false;
                        histogram_values.put(TelemetryConstants.VALID,"false");
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM,1L);
                    }
                        
                } else {
                    pigCounterHelper.incrCounter(ReportStats.UNKNOWN_HISTOGRAMS,1L);
                }
            }
            if (validForSchema) {
                info.put(TelemetryConstants.VALID_FOR_SCHEMA,"true");
            } else {
                info.put(TelemetryConstants.VALID_FOR_SCHEMA,"false");
            }
            return jsonMapper.writeValueAsString(crash);
        } catch (JsonParseException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
            pigCounterHelper.incrCounter(ReportStats.INVALID_JSON_STRUCTURE,1L);
        } catch (JsonMappingException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
            pigCounterHelper.incrCounter(ReportStats.INVALID_JSON_STRUCTURE,1L);
        } catch (IOException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    void dump_histogram_values(String key,Map<String,Object> referenceHistograms) {
        LOG.info(key);
        for(Map.Entry<String,Object> k : referenceHistograms.entrySet()) {
            if(!k.equals("values")) {
                LOG.info(k.getKey() + " " +k.getValue());
            }
        }

        if(referenceHistograms.containsKey("values")) {
            Map<String,Object> bucket_values = (LinkedHashMap<String,Object>)referenceHistograms.get("values");
            for(Map.Entry<String,Object> bk : bucket_values.entrySet()) {
                LOG.info(bk.getKey());
            }
        }
    }
}
 
