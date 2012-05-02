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
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
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

public class AggregateElasticSearchIndexer {

    private static final Logger LOG = Logger.getLogger(AggregateElasticSearchIndexer.class);
    
    private static final int VALID_ROW_SIZE = 19;
    private static final int DATE_IDX = 0;
    private static final int PRODUCT_IDX = 1;
    private static final int PRODUCT_VERSION_IDX = 2;
    private static final int CHANNEL_IDX = 3;
    private static final int ARCH_IDX = 4;
    private static final int OS_IDX = 5;
    private static final int OS_VERSION_IDX = 6;
    private static final int APP_BUILD_ID_IDX = 7;
    private static final int PLAT_BUILD_ID_IDX = 8;
    private static final int HIST_NAME_IDX = 9;
    private static final int HIST_VALUE_IDX = 10;
    private static final int BUCKET_COUNT_IDX = 11;
    private static final int MIN_RANGE_IDX = 12;
    private static final int MAX_RANGE_IDX = 13;
    private static final int HIST_TYPE_IDX = 14;
    private static final int VALUE_SUM_COUNT_IDX = 15;
    private static final int VALUE_SUM_SUM_IDX = 16;
    private static final int VALUE_DOC_COUNT_IDX = 17; // currently field is not used but this could prove useful in the future
    private static final int HIST_NAME_DOC_COUNT_IDX = 18;
    
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
                        for (int i=DATE_IDX; i <= PLAT_BUILD_ID_IDX; i++) {
                            if (!prevSplits[i].equals(splits[i])) {
                                startNewObject = true;
                                break;
                            }
                        }
                    }
 
                    try {
                        if (splits.length == VALID_ROW_SIZE) {
                            if (startNewObject) {
                                // Write out previous object if there is one
                                if (tdata != null) {
                                    brb.add(client.prepareIndex(indexName, typeName).setSource(jsonMapper.writeValueAsBytes(tdata)));
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
                                tdata.setDate(splits[DATE_IDX]);
                                TelemetryDataAggregate.Info info = new TelemetryDataAggregate.Info();
                                info.setAppName(splits[PRODUCT_IDX]);
                                info.setAppVersion(splits[PRODUCT_VERSION_IDX]);
                                if ("NA".equals(splits[CHANNEL_IDX])) {
                                    info.setAppUpdateChannel("");
                                } else {
                                    info.setAppUpdateChannel(splits[CHANNEL_IDX]);
                                }
                                info.setArch(splits[ARCH_IDX]);
                                info.setOS(splits[OS_IDX]);
                                info.setVersion(splits[OS_VERSION_IDX]);
                                info.setAppBuildId(splits[APP_BUILD_ID_IDX]);
                                info.setPlatformBuildId(splits[PLAT_BUILD_ID_IDX]);
                                tdata.setInfo(info);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s", tdata.getDate(), tdata.getInfo().getAppName(), 
                                                tdata.getInfo().getAppVersion(), tdata.getInfo().getAppUpdateChannel(), tdata.getInfo().getArch(),
                                                tdata.getInfo().getOS(), tdata.getInfo().getVersion(), tdata.getInfo().getAppBuildId(),
                                                tdata.getInfo().getPlatformBuildId()));
                                }
                            }
                            
                            // Make value int safe
                            String safeValue = splits[HIST_VALUE_IDX].replaceAll("[^0-9]", "");
                            String histName = splits[HIST_NAME_IDX];
                            // Add histogram entry
                            tdata.addOrPutHistogramValue(histName, safeValue, (long)Double.parseDouble(splits[VALUE_SUM_COUNT_IDX]));
                            // set histogram count
                            tdata.setHistogramCount(histName, Integer.parseInt(splits[HIST_NAME_DOC_COUNT_IDX]));
                            // set the histogram sum
                            tdata.setHistogramSum(histName, (long)Double.parseDouble(splits[VALUE_SUM_SUM_IDX]));
                            // set the histogram bucket count
                            tdata.setHistogramBucketCount(histName, Integer.parseInt(splits[BUCKET_COUNT_IDX]));
                            // set the hisotgram range
                            tdata.setHistogramRange(histName, Integer.parseInt(splits[MIN_RANGE_IDX]), Integer.parseInt(splits[MAX_RANGE_IDX]));
                            // set the histogram type
                            tdata.setHistogramType(histName, Integer.parseInt(splits[HIST_TYPE_IDX]));
                        } else {
                            LOG.error("Encountered invalid split length for line: " + line);
                        }
                    } catch (NumberFormatException e) {
                        LOG.warn("Encountered a bad number", e);
                        LOG.warn("Splits: ");
                        for (String s : splits) {
                            LOG.warn("\t" + s);
                        }
                    } catch (Exception e) {
                        LOG.error("Generic exception catch", e);
                    }
                    
                    prevSplits = splits;
                }
                
                // Index remaining docs if there are any
                if (tdata != null) {
                    brb.add(client.prepareIndex(indexName, typeName).setSource(jsonMapper.writeValueAsBytes(tdata)));
                }
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
