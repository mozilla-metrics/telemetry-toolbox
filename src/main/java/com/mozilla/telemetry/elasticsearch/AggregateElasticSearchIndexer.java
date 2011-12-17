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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.mozilla.telemetry.elasticsearch.TelemetryDataAggregate.Histogram;

public class AggregateElasticSearchIndexer {

    private static final Logger LOG = Logger.getLogger(AggregateElasticSearchIndexer.class);
    
    private Configuration conf;
    private FileSystem fs;
    
    private String indexName;
    private String typeName;
    private String aliasName;
    private Node node;
    private Client client;
    private AdminClient adminClient;
    
    public AggregateElasticSearchIndexer(String indexName, String typeName, String aliasName) throws IOException {
        this.conf = new Configuration();
        this.fs = FileSystem.get(conf);
        
        this.node = NodeBuilder.nodeBuilder().loadConfigSettings(true).client(true).node();
        this.client = node.client();
        this.indexName = indexName;
        this.typeName = typeName;
        this.aliasName = aliasName;
    }
    
    public void close() {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (client != null) {
            client.close();
        }
        if (node != null) {
            node.close();
        }
    }
    
    public void prepareIndexForBulk() {
        // Make sure to have the index available.
        adminClient = client.admin();
        if (adminClient.indices().prepareExists(indexName).execute().actionGet().exists()) {
            LOG.info(String.format("Deleting existing index '%s'", indexName));
            boolean success = adminClient.indices().prepareDelete(indexName).execute().actionGet().acknowledged();
            LOG.info(String.format("delete index: %s", (success ? "succeeded" : "failed")));
        }
        
        LOG.info(String.format("Creating index '%s'", indexName));
        Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_replicas", 0).build();
        boolean success = adminClient.indices().prepareCreate(indexName).setSettings(settings).execute().actionGet().acknowledged();
        LOG.info(String.format("index.number_of_replicas set to 0: %s", (success ? "succeeded" : "failed")));
        
        LOG.info(String.format("Waiting for index '%s'...", indexName));
        adminClient.cluster().prepareHealth(indexName).setWaitForNodes(">0").execute().actionGet();
        
        // Set refresh interval to -1
        Settings updateSettings = ImmutableSettings.settingsBuilder()
                                    .put("index.refresh_interval", "-1")
                                    .put("merge.policy.merge_factor", 30).build();
        adminClient.indices().prepareUpdateSettings(indexName).setSettings(updateSettings).execute().actionGet();
        LOG.info("index.refresh_interval set to -1");
    }
    
    public void prepareIndexForQuery() {
        // Set replicas back to 1 and refresh interval to 1s
        Settings settings = ImmutableSettings.settingsBuilder()
                                //.put("index.number_of_replicas", 1)
                                .put("index.refresh_interval", "1s")
                                .put("merge.policy.merge_factor", 10).build();
        adminClient.indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
        LOG.info("index.refresh_interval set to 1s");
        
        if (aliasName != null) {
            boolean success = adminClient.indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet().acknowledged();
            LOG.info(String.format("index alias creation: %s", (success ? "succeeded" : "failed")));
        }
    }
    
    /**
     * Corrects buckets if there are more actual buckets than configured buckets for
     * a given histogram
     * @param tdata
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private boolean checkAndCorrectBuckets(TelemetryDataAggregate tdata) {
        LOG.info(String.format("Checking buckets for %s %s (%s,%s) on %s %s", tdata.getInfo().getAppName(), tdata.getInfo().getAppVersion(), tdata.getInfo().getAppBuildId(), tdata.getInfo().getPlatformBuildId(), tdata.getInfo().getOS(), tdata.getInfo().getVersion()));
        
        boolean wasCorrected = false;
        for (Map.Entry<String,Histogram> entry : tdata.getHistograms().entrySet()) {
            String histName = entry.getKey();
            Histogram hist = entry.getValue();
            List<int[]> values = hist.getValues();
            if (hist.getBucketCount() < values.size() && !histName.startsWith("SIMPLE_MEASURES")) {
                
                LOG.info(String.format("%s histogram was configured for %d buckets but actually has %d.", entry.getKey(), hist.getBucketCount(), values.size()));
                
                // Sort by counts
                Collections.sort(values, Collections.reverseOrder(new Comparator() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        int[] a1 = (int[])o1;
                        int[] a2 = (int[])o2;
                        
                        if (a1.length == 2 && a2.length == 2) {
                            return a1[1] < a2[1] ? -1 : a1[1] > a2[1] ? 1 : 0;
                        }
                        
                        return 0;
                    }         
                }));
                
                // All values beyond the hist bucket count at this point need to be combined into neighboring buckets
                List<int[]> mergeValues = values.subList(hist.getBucketCount(), values.size());
                Set<Integer> mergeBuckets = new HashSet<Integer>();
                for (int i=0; i < mergeValues.size(); i++) {
                    int[] vs = mergeValues.get(i);
                    mergeBuckets.add(vs[0]);
                }
                
                // Sort by buckets
                Collections.sort(values, new Comparator() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        int[] a1 = (int[])o1;
                        int[] a2 = (int[])o2;
                        
                        if (a1.length == 2 && a2.length == 2) {
                            return a1[0] < a2[0] ? -1 : a1[0] > a2[0] ? 1 : 0;
                        }
                        
                        return 0;
                    }         
                });
                
                // Merge process
                for (int i=0; i <= hist.getBucketCount() && i < values.size(); i++) {
                    int[] cur = values.get(i);
                    // If this bucket is in the merge set then merge it into it's neighbor above
                    // or below in the case it is the last bucket
                    if (mergeBuckets.contains(cur[0])) {
                        if ((i+1) < values.size()) {
                            int[] above = values.get(i+1);
                            LOG.info(String.format("Merging bucket %d=%d into %d=%d",cur[0],cur[1],above[0],above[1]));
                            above[1] += cur[1];
                            values.set(i+1, above);
                        } else {
                            int[] below = values.get(i-1);
                            LOG.info(String.format("Merging bucket %d=%d into %d=%d",cur[0],cur[1],below[0],below[1]));
                            below[1] += cur[1];
                            values.set(i-1, below);
                        }
                        
                        // Remove this bucket from the list
                        values.remove(i);
                    }
                }

                wasCorrected = true;
            }
        }
        
        return wasCorrected;
    }
    
    public void indexHDFSData(String inputPath) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        Pattern tab = Pattern.compile("\t");
        int counter = 0;
        
        for (FileStatus status : fs.listStatus(new Path(inputPath))) {
            if (status.isDir()) {
                continue;
            }
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
                String line = null;
                String[] prevSplits = null;
                TelemetryDataAggregate tdata = null;
                BulkRequestBuilder brb = client.prepareBulk();
                while ((line = reader.readLine()) != null) {
                    String[] splits = tab.split(line);
                    boolean startNewObject = false;
                    if (prevSplits == null) {
                        startNewObject = true;
                    } else if (prevSplits.length == splits.length) {
                        for (int i=0; i <= 7; i++) {
                            if (!prevSplits[i].equals(splits[i])) {
                                startNewObject = true;
                            }
                        }
                    }
                    
                    if (startNewObject) {
                        // Write out previous object if there is one
                        if (tdata != null) {
                            // Check the buckets and correct them by merging
//                            if (checkAndCorrectBuckets(tdata)) {
//                                System.out.println(jsonMapper.writeValueAsString(tdata));
//                            }
                            ByteArrayOutputStream baos = new ByteArrayOutputStream();
                            jsonMapper.writeValue(baos, tdata);

                            // Add this JSON data to BulkRequest
                            brb.add(client.prepareIndex(indexName, typeName).setSource(baos.toByteArray()));
                            if (brb.numberOfActions() >= 100) {
                                int numActions = brb.numberOfActions();
                                LOG.info("Sending BulkRequest ...");
                                BulkResponse response = brb.execute().actionGet();
                                LOG.info(String.format("BulkRequest took: %d ms", response.getTookInMillis()));
                                if (response.hasFailures()) {
                                    for (BulkItemResponse b : response) {
                                        LOG.error("Failed on id: " + b.getId() + " message: " + b.getFailureMessage());
                                    }
                                    break;
                                }
                                
                                counter += numActions;
                                LOG.info("Total documents indexed: " + counter);
                                
                                // Reset the bulk request object
                                brb = client.prepareBulk();
                            }
                        }
                        
                        tdata = new TelemetryDataAggregate();
                        tdata.setDate(splits[0]);
                        TelemetryDataAggregate.Info info = new TelemetryDataAggregate.Info();
                        info.setAppName(splits[1]);
                        info.setAppVersion(splits[2]);
                        info.setArch(splits[3]);
                        info.setOS(splits[4]);
                        info.setVersion(splits[5]);
                        info.setAppBuildId(splits[6]);
                        info.setPlatformBuildId(splits[7]);
                        tdata.setInfo(info);
                    }
                    
                    try {
                        // Make value int safe
                        String safeValue = splits[9].replaceAll("[a-zA-Z]", "");
                        
                        // Add histogram entry
                        tdata.addOrPutHistogramValue(splits[8], safeValue, (int)Float.parseFloat(splits[14]));
                        // increment histogram count
                        tdata.incrementHistogramCount(splits[8], Integer.parseInt(splits[15]));
                        // set the histogram sum
                        tdata.setHistogramSum(splits[8], (long)Double.parseDouble(splits[16]));
                        // set the histogram bucket count
                        tdata.setHistogramBucketCount(splits[8], Integer.parseInt(splits[10]));
                        // set the hisotgram range
                        tdata.setHistogramRange(splits[8], Integer.parseInt(splits[11]), Integer.parseInt(splits[12]));
                        // set the histogram type
                        tdata.setHistogramType(splits[8], Integer.parseInt(splits[13]));
                    } catch (NumberFormatException e) {
                        LOG.warn("Encountered a bad number", e);
                        LOG.warn("Splits: ");
                        for (String s : splits) {
                            LOG.warn("\t" + s);
                        }
                        throw e;
                    }
                    
                    prevSplits = splits;
                }
                
                // Index remaining docs if there are any
                if (brb.numberOfActions() > 0) {
                    int numActions = brb.numberOfActions();
                    BulkResponse response = brb.execute().actionGet();
                    LOG.info(String.format("BulkRequest took: %d ms", response.getTookInMillis()));
                    if (response.hasFailures()) {
                        for (BulkItemResponse b : response) {
                            LOG.error("Failed on id: " + b.getId() + " message: " + b.getFailureMessage());
                        }
                        break;
                    }
                    
                    counter += numActions;
                    LOG.info("Total documents indexed: " + counter);
                }
            } finally {
                if (reader != null) {
                    reader.close();
                }
            }
        }
    }
    
    public static void main(String[] args) throws IOException, ParseException {        
        AggregateElasticSearchIndexer esi = null;
        try {
            esi = new AggregateElasticSearchIndexer("telemetry_agg_"+args[0], "data", "telemetry");
            esi.prepareIndexForBulk();
            esi.indexHDFSData(args[1]);
            esi.prepareIndexForQuery();
        } finally {
            if (esi != null) {
                esi.close();
            }
        }
    }
}
