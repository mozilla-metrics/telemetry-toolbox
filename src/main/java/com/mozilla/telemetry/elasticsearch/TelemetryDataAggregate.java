/*
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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.telemetry.elasticsearch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonProperty;

@JsonAutoDetect(getterVisibility=Visibility.NONE)
public class TelemetryDataAggregate {

    @JsonAutoDetect(getterVisibility=Visibility.NONE)
    public static class Info {
        @JsonProperty("appName")
        private String appName;
        @JsonProperty("appVersion")
        private String appVersion;
        @JsonProperty("appUpdateChannel")
        private String appUpdateChannel;
        @JsonProperty("OS")
        private String OS;
        @JsonProperty("appBuildID")
        private String appBuildId;
        @JsonProperty("platformBuildID")
        private String platformBuildId;
        @JsonProperty("arch")
        private String arch;
        @JsonProperty("version")
        private String version;
        
        public String getAppName() {
            return appName;
        }
        public void setAppName(String appName) {
            this.appName = appName;
        }
        public String getAppVersion() {
            return appVersion;
        }
        public void setAppVersion(String appVersion) {
            this.appVersion = appVersion;
        }
        public String getAppUpdateChannel() {
            return appUpdateChannel;
        }
        public void setAppUpdateChannel(String appUpdateChannel) {
            this.appUpdateChannel = appUpdateChannel;
        }
        public String getOS() {
            return OS;
        }
        public void setOS(String os) {
            OS = os;
        }
        public String getAppBuildId() {
            return appBuildId;
        }
        public void setAppBuildId(String appBuildId) {
            this.appBuildId = appBuildId;
        }
        public String getPlatformBuildId() {
            return platformBuildId;
        }
        public void setPlatformBuildId(String platformBuildId) {
            this.platformBuildId = platformBuildId;
        }
        public String getArch() {
            return arch;
        }
        public void setArch(String arch) {
            this.arch = arch;
        }
        public String getVersion() {
            return version;
        }
        public void setVersion(String version) {
            this.version = version;
        }
    }
    
    @JsonAutoDetect(getterVisibility=Visibility.NONE)
    public static class Histogram {
        
        @JsonProperty("values")
        private List<long[]> values = new ArrayList<long[]>();
        @JsonProperty("count")
        private int count;
        @JsonProperty("sum")
        private long sum;
        @JsonProperty("bucket_count")
        private int bucketCount;
        @JsonProperty("range")
        private int[] range = new int[2];
        @JsonProperty("histogram_type")
        private int histogramType;
        
        public List<long[]> getValues() {
            return values;
        }

        public void addValue(long[] value) {
            this.values.add(value);
        }
        
        public void setValues(List<long[]> values) {
            this.values = values;
        }

        public int getCount() {
            return count;
        }

        public void incrementCount(int count) {
            this.count += count;
        }
        
        public void setCount(int count) {
            this.count = count;
        }

        public long getSum() {
            return sum;
        }

        public void setSum(long sum) {
            this.sum = sum;
        }

        public int getBucketCount() {
            return bucketCount;
        }

        public void setBucketCount(int bucketCount) {
            this.bucketCount = bucketCount;
        }

        public int[] getRange() {
            return range;
        }

        public void setMinMaxRange(int minRange, int maxRange) {
            range[0] = minRange;
            range[1] = maxRange;
        }
        
        public void setRange(int[] range) {
            this.range = range;
        }

        public int getHistogramType() {
            return histogramType;
        }

        public void setHistogramType(int histogramType) {
            this.histogramType = histogramType;
        }
    }
    
    @JsonProperty("date")
    private String date;
    @JsonProperty("info")
    private Info info;
    @JsonProperty("histogram_names")
    private Set<String> histogramNames = new HashSet<String>();
    @JsonProperty("histograms")
    private Map<String,Histogram> histograms = new HashMap<String,Histogram>();
    
    public String getDate() {
        return date;
    }
    
    public void setDate(String date) {
        this.date = date;
    }
    
    public Info getInfo() {
        return info;
    }

    public void setInfo(Info info) {
        this.info = info;
    }

    public void setHistograms(Map<String, Histogram> histograms) {
        this.histograms = histograms;
    }
    
    public Map<String, Histogram> getHistograms() {
        return histograms;
    }

    public void addOrPutHistogramValue(String key, String histValueKey, Long histValue) {
        Histogram hist = null;
        if (histograms.containsKey(key)) {
            hist = histograms.get(key);
        } else {
            hist = new Histogram();
        }

        if (!"".equals(histValueKey.trim())) {
            hist.addValue(new long[] { Long.parseLong(histValueKey), histValue });
            histograms.put(key, hist);
            histogramNames.add(key);
        }
    }
    
    public void setHistogramCount(String key, int count) {
        Histogram hist = null;
        if (histograms.containsKey(key)) {
            hist = histograms.get(key);
        } else {
            hist = new Histogram();
        }
        
        hist.setCount(count);
        histograms.put(key, hist);
    }

    public void setHistogramSum(String key, long sum) {
        Histogram hist = null;
        if (histograms.containsKey(key)) {
            hist = histograms.get(key);
        } else {
            hist = new Histogram();
        }
        
        hist.setSum(sum);
        histograms.put(key, hist);
    }
    
    public void setHistogramBucketCount(String key, int bucketCount) {
        Histogram hist = null;
        if (histograms.containsKey(key)) {
            hist = histograms.get(key);
        } else {
            hist = new Histogram();
        }
        
        hist.setBucketCount(bucketCount);
        histograms.put(key, hist);
    }
    
    public void setHistogramRange(String key, int minRange, int maxRange) {
        Histogram hist = null;
        if (histograms.containsKey(key)) {
            hist = histograms.get(key);
        } else {
            hist = new Histogram();
        }
        
        hist.setMinMaxRange(minRange, maxRange);
        histograms.put(key, hist);
    }
    
    public void setHistogramType(String key, int histogramType) {
        Histogram hist = null;
        if (histograms.containsKey(key)) {
            hist = histograms.get(key);
        } else {
            hist = new Histogram();
        }
        
        hist.setHistogramType(histogramType);
        histograms.put(key, hist);
    }

    public Set<String> getHistogramNames() {
        return histogramNames;
    }

    public void setHistogramNames(Set<String> histogramNames) {
        this.histogramNames = histogramNames;
    }
    
}
