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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import com.mozilla.telemetry.constants.TelemetryConstants;
import com.twitter.elephantbird.pig.util.PigCounterHelper;

public class ValidateTelemetrySubmission extends EvalFunc<String> {
    
    static final Logger LOG = Logger.getLogger(ValidateTelemetrySubmission.class);
    
    private PigCounterHelper pigCounterHelper = new PigCounterHelper();
    
    private ObjectMapper jsonMapper;
    Map<String, Object> specValues = null;
    final String lookupFilename;
    
    enum ReportStats {
        VALID_HISTOGRAM, INVALID_HISTOGRAM, INVALID_JSON_STRUCTURE, INVALID_SUBMISSIONS, 
        KNOWN_HISTOGRAMS, UNKNOWN_HISTOGRAMS, META_DATA_INVALID, UNDEFINED_HISTOGRAMS, 
        MISSING_JSON_REFERENCE, SUBMISSIONS_EVALUATED, SUBMISSIONS_SKIPPED, 
        MISSING_JSON_VALUES_FIELD, JSON_INVALID_VALUES_FIELD, INVALID_HISTOGRAM_BUCKET_VALUE, 
        INVALID_HISTOGRAM_BUCKET_COUNT, INVALID_HISTOGRAM_MAX, INVALID_HISTOGRAM_MIN, 
        INVALID_HISTOGRAM_TYPE, NO_HISTOGRAM_BUCKET_VALUES
    };

    public ValidateTelemetrySubmission(String filename) {
        lookupFilename = filename;
        jsonMapper = new ObjectMapper();
    }

    @Override
    public String exec(Tuple input) throws IOException {
        if (specValues == null) {
            readLookupFile();
        }
        DataByteArray key = (DataByteArray) input.get(0);
        String json = (String) input.get(1);
        if (json == null) {
            LOG.info("json is null for key: " + key.toString());
            pigCounterHelper.incrCounter(ReportStats.META_DATA_INVALID, 1L);
        } else {
            String newJson = validateTelemetryJson(json);
            if (newJson != null) {
                return newJson;
            }
        }
        return null;
    }

    protected FSDataInputStream getHDFSFile(String fileName) throws IOException {
        FileSystem fs = FileSystem.get(UDFContext.getUDFContext().getJobConf());
        return fs.open(new Path(fileName));
    }

    protected void readLookupFile() {
        FSDataInputStream fdis = null;
        try {
            Properties specProperties = new Properties();
            specValues = new HashMap<String, Object>();
            fdis = getHDFSFile(lookupFilename);
            specProperties.load(fdis);
            for (String key : specProperties.stringPropertyNames()) {
                String specFile = specProperties.getProperty(key);
                LOG.info(key + " " + specFile);
                Map<String, Map<String, Object>> referenceJson = readReferenceJson(specFile);
                specValues.put(key, referenceJson);
            }
        } catch (IOException e) {
            LOG.error("ERROR: failed to process telemetry spec lookup file " + e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("ERROR: failed to process telemetry spec jsons " + e.getMessage());
        } finally {
            if (fdis != null) {
                try {
                    fdis.close();
                } catch (IOException e) {
                    LOG.error("ERROR: failed to close telemetry spec lookup file" + e.getMessage());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Map<String, Object>> readReferenceJson(String filename) {
        FSDataInputStream fdis = null;
        try {
            Map<String, Map<String, Object>> referenceValues = new HashMap<String, Map<String, Object>>();
            fdis = getHDFSFile(filename);
            Map<String, Object> referenceJson = jsonMapper.readValue(fdis, new TypeReference<Map<String, Object>>() {});
            Map<String, Object> histograms = (Map<String, Object>)referenceJson.get(TelemetryConstants.HISTOGRAMS);

            for (Map.Entry<String, Object> histogram : histograms.entrySet()) {
                Map<String, Object> compKey = new HashMap<String, Object>();
                String jKey = histogram.getKey();
                Map<String, Object> histogramValues = (Map<String, Object>)histogram.getValue();
                compKey.put(TelemetryConstants.NAME, jKey);
                if (histogramValues.containsKey(TelemetryConstants.MIN)) {
                    compKey.put(TelemetryConstants.MIN, String.valueOf(histogramValues.get(TelemetryConstants.MIN)));
                }
                if (histogramValues.containsKey(TelemetryConstants.MAX)) {
                    compKey.put(TelemetryConstants.MAX, String.valueOf(histogramValues.get(TelemetryConstants.MAX)));
                }
                if (histogramValues.containsKey(TelemetryConstants.KIND)) {
                    compKey.put(TelemetryConstants.HISTOGRAM_TYPE, String.valueOf(histogramValues.get(TelemetryConstants.KIND)));
                }
                if (histogramValues.containsKey(TelemetryConstants.BUCKET_COUNT)) {
                    compKey.put(TelemetryConstants.BUCKET_COUNT, String.valueOf(histogramValues.get(TelemetryConstants.BUCKET_COUNT)));
                }
                if (histogramValues.containsKey(TelemetryConstants.BUCKETS)) {
                    compKey.put(TelemetryConstants.BUCKETS, (List<Integer>)histogramValues.get(TelemetryConstants.BUCKETS));
                }

                referenceValues.put(jKey, compKey);
            }
            
            return referenceValues;
        } catch (IOException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons " + e.getMessage());
        } finally {
            if (fdis != null) {
                try {
                    fdis.close();
                } catch (IOException e) {
                    LOG.error("ERROR: failed to close input stream to reference json" + e.getMessage());
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected String getAppVersionFromTelemetryDoc(Map<String, Object> jsonMap) {
        Map<String, Object> infoValue = (Map<String, Object>)jsonMap.get(TelemetryConstants.INFO);
        String appVersion = null;
        try {
            appVersion = (String) infoValue.get(TelemetryConstants.APP_VERSION);
        } catch (Exception e) {
            LOG.info("ERROR: no appversion in telemetry submission " + e.getMessage());
        }
        return appVersion;
    }

    protected boolean checkVersion(String appVersion) {
        boolean appVersionMatch = false;
        for (Map.Entry<String, Object> entry : specValues.entrySet()) {
            if (appVersion.contains(entry.getKey())) {
                appVersionMatch = true;
                break;
            }
        }
        return appVersionMatch;
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Map<String, Object>> getJsonSpec(String appVersion) {
        for (Map.Entry<String, Object> entry : specValues.entrySet()) {
            if (appVersion.contains(entry.getKey())) {
                return (Map<String, Map<String, Object>>) specValues.get(entry.getKey());
            }
        }
        return (Map<String, Map<String, Object>>) specValues.get("default");
    }

    @SuppressWarnings("unchecked")
    protected String validateTelemetryJson(String json) {
        try {
            Map<String, Object> jsonMap = jsonMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            String appVersion = getAppVersionFromTelemetryDoc(jsonMap);
            Map<String, Object> info = (Map<String, Object>)jsonMap.get(TelemetryConstants.INFO);
            if (info == null) {
                info = new LinkedHashMap<String, Object>();
                jsonMap.put(TelemetryConstants.INFO, info);
            }

            Map<String, Map<String, Object>> referenceValues = getJsonSpec(appVersion);
            if (referenceValues == null) {
                LOG.info("referenceValues is null " + appVersion);
            }
            pigCounterHelper.incrCounter(ReportStats.SUBMISSIONS_EVALUATED, 1L);

            Map<String, Object> histograms = (Map<String, Object>)jsonMap.get(TelemetryConstants.HISTOGRAMS);
            if (histograms == null || appVersion == null) {
                info.put(TelemetryConstants.VALID_FOR_SCHEMA, "false");
                return jsonMapper.writeValueAsString(jsonMap);
            }
            boolean validForSchema = true;
            for (Map.Entry<String, Object> entry : histograms.entrySet()) {
                String jKey = entry.getKey();
                String min = new String();
                String max = new String();
                String histogramType = new String();
                String bucketCount = new String();
                boolean validHistogram = true;
                Map<String, Object> bucketValues = new LinkedHashMap<String, Object>();
                LinkedHashMap<String, Object> histogramValues = (LinkedHashMap<String, Object>)entry.getValue();

                for (Map.Entry<String, Object> histogramValue : histogramValues.entrySet()) {
                    try {
                        if (StringUtils.equals(histogramValue.getKey(), TelemetryConstants.RANGE)) {
                            List<Integer> range = (List<Integer>) histogramValue.getValue();
                            min = String.valueOf(range.get(0));
                            max = String.valueOf(range.get(1));
                        } else if (StringUtils.equals(histogramValue.getKey(), TelemetryConstants.HISTOGRAM_TYPE)) {
                            histogramType = String.valueOf(histogramValue.getValue());
                        } else if (StringUtils.equals(histogramValue.getKey(), TelemetryConstants.BUCKET_COUNT)) {
                            bucketCount = String.valueOf(histogramValue.getValue());
                        } else if (StringUtils.equals(histogramValue.getKey(), TelemetryConstants.VALUES)) {
                            bucketValues = (Map<String, Object>)histogramValue.getValue();
                        }
                    } catch (Exception e) {
                        LOG.error(histogramValue, e);
                    }
                }

                if (referenceValues.containsKey(jKey)) {
                    pigCounterHelper.incrCounter(ReportStats.KNOWN_HISTOGRAMS, 1L);
                    Map<String, Object> referenceHistograms = referenceValues.get(jKey);
                    String referenceHistogramType = (String) referenceHistograms.get(TelemetryConstants.HISTOGRAM_TYPE);

                    if (!StringUtils.equals(referenceHistogramType, histogramType)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_TYPE, 1L);
                    }
                    if (!StringUtils.equals((String)referenceHistograms.get(TelemetryConstants.MIN), min)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_MIN, 1L);
                    }
                    if (!StringUtils.equals((String)referenceHistograms.get(TelemetryConstants.MAX), max)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_MAX, 1L);
                    }
                    if (!StringUtils.equals((String)referenceHistograms.get(TelemetryConstants.BUCKET_COUNT), bucketCount)) {
                        validHistogram = false;
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_BUCKET_COUNT, 1L);
                    }
                    if (bucketValues == null || bucketValues.size() <= 0) {
                        pigCounterHelper.incrCounter(ReportStats.NO_HISTOGRAM_BUCKET_VALUES, 1L);
                        validHistogram = false;
                    } else {
                        LinkedHashMap<String, Integer> invalidValues = new LinkedHashMap<String, Integer>();
                        List<Integer> referenceBucketValues = (List<Integer>) referenceHistograms.get(TelemetryConstants.BUCKETS);
                        for (Map.Entry<String, Object> bucketValue : bucketValues.entrySet()) {
                            int bucketKey = -1;
                            try {
                                bucketKey = Integer.parseInt(bucketValue.getKey());
                            } catch (Exception e) {
                                bucketKey = -1;
                            }
                            if (!referenceBucketValues.contains(bucketKey)) {
                                invalidValues.put(TelemetryConstants.VALUES, bucketKey);
                            }
                        }

                        if (invalidValues.size() > 0) {
                            pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM_BUCKET_VALUE, 1L);
                            validHistogram = false;
                        }
                        invalidValues = null;
                    }

                    if (validHistogram) {
                        histogramValues.put(TelemetryConstants.VALID, "true");
                        pigCounterHelper.incrCounter(ReportStats.VALID_HISTOGRAM, 1L);
                    } else {
                        validForSchema = false;
                        histogramValues.put(TelemetryConstants.VALID, "false");
                        pigCounterHelper.incrCounter(ReportStats.INVALID_HISTOGRAM, 1L);
                    }

                } else {
                    pigCounterHelper.incrCounter(ReportStats.UNKNOWN_HISTOGRAMS, 1L);
                }
            }
            if (validForSchema) {
                info.put(TelemetryConstants.VALID_FOR_SCHEMA, "true");
            } else {
                info.put(TelemetryConstants.VALID_FOR_SCHEMA, "false");
            }
            return jsonMapper.writeValueAsString(jsonMap);
        } catch (JsonParseException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons " + e.getMessage());
            pigCounterHelper.incrCounter(ReportStats.INVALID_JSON_STRUCTURE, 1L);
        } catch (JsonMappingException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons " + e.getMessage());
            pigCounterHelper.incrCounter(ReportStats.INVALID_JSON_STRUCTURE, 1L);
        } catch (IOException e) {
            LOG.info("ERROR: failed to process telemetry spec jsons " + e.getMessage());
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    void dumpHistogramValues(String key, Map<String, Object> referenceHistograms) {
        LOG.info(key);
        for (Map.Entry<String, Object> k : referenceHistograms.entrySet()) {
            if (!k.equals("values")) {
                LOG.info(k.getKey() + " " + k.getValue());
            }
        }

        if (referenceHistograms.containsKey("values")) {
            Map<String, Object> bucketValues = (Map<String, Object>)referenceHistograms.get("values");
            for (Map.Entry<String, Object> bk : bucketValues.entrySet()) {
                LOG.info(bk.getKey());
            }
        }
    }
}
