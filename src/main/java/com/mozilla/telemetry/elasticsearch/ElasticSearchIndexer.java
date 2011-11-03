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

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.action.bulk.BulkRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;

import com.mozilla.hadoop.hbase.mapreduce.MultiScanTableMapReduceUtil;

/**
 * This is a temporary class made to take telemetry data in HBase and index in ElasticSearch. In the future 
 * we will simply use MultiMapStore instead.
 */
public class ElasticSearchIndexer {

    private static final Logger LOG = Logger.getLogger(ElasticSearchIndexer.class);
    
    private static final int BATCH_SIZE = 100;
    
    private final String indexName;
    private final String type;
    
    private final String tableName;
    private final byte[] columnFamily;
    private final byte[] columnQualifier;
    
    private Node node;
    private Client client;
    private HTablePool hbasePool;
    private AdminClient adminClient;
    
    private final JsonFactory jsonFactory = new JsonFactory();

    public ElasticSearchIndexer() {
        this.indexName = "";
        this.type = "";
        this.tableName = "";
        this.columnFamily = new byte[0];
        this.columnQualifier = new byte[0];
    }
    
    public ElasticSearchIndexer(String indexName, String type, String tableName, String columnFamily, String columnQualifier) {
        this.node = nodeBuilder().loadConfigSettings(true).client(true).node();
        this.client = node.client();
        this.indexName = indexName;
        this.type = type;
        
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.regionserver.lease.period", String.valueOf(600000));
        this.hbasePool = new HTablePool(conf, 32);
        
        this.tableName = tableName;
        this.columnFamily = columnFamily.getBytes();
        this.columnQualifier = columnQualifier.getBytes();
    }
    
    public void close() {
        if (client != null) {
            client.close();
        }
        if (node != null) {
            node.close();
        }
        if (hbasePool != null && tableName != null) {
            hbasePool.closeTablePool(tableName);
        }
    }

    public void prepareIndexForBulk() {
        // Make sure to have the index available.
        adminClient = client.admin();
        if (!adminClient.indices().exists(new IndicesExistsRequest(indexName)).actionGet().exists()) {
            LOG.info(String.format("Creating missing index '%s'", indexName));
            Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_replicas", 0).build();
            boolean success = adminClient.indices().create(new CreateIndexRequest(indexName).settings(settings)).actionGet().acknowledged();
            LOG.info(String.format("index.number_of_replicas set to 0: %s", (success ? "succeeded" : "failed")));
        }
        LOG.info(String.format("Waiting for index '%s'...", indexName));
        adminClient.cluster().health(new ClusterHealthRequest(indexName).waitForNodes("1")).actionGet();
        
        // Set refresh interval to -1
        Settings settings = ImmutableSettings.settingsBuilder()
                                .put("index.refresh_interval", "-1")
                                .put("merge.policy.merge_factor", 30).build();
        adminClient.indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
        LOG.info("index.refresh_interval set to -1");
    }
    
    public void prepareIndexForQuery() {
        // Set replicas back to 1
        Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_replicas", 1).build();
        boolean success = adminClient.indices().create(new CreateIndexRequest(indexName).settings(settings)).actionGet().acknowledged();
        LOG.info(String.format("index.number_of_replicas set to 1: %s", (success ? "succeeded" : "failed")));
        
        // Set refresh interval to -1
        settings = ImmutableSettings.settingsBuilder()
                        .put("index.refresh_interval", "1s")
                        .put("merge.policy.merge_factor", 10).build();
        adminClient.indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
        LOG.info("index.refresh_interval set to 1s");
        
        // Call optimize
        OptimizeResponse or = adminClient.indices().prepareOptimize(indexName).setMaxNumSegments(5).execute().actionGet();
        LOG.info(String.format("optimize.max_num_segments set to 5: %s", (or.successfulShards() == or.failedShards() ? "succeeded" : "failed")));
    }
    
    public byte[] modifyJSON(String date, byte[] data) throws JsonParseException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8096);
        JsonParser parser = null;
        JsonGenerator generator = null;
        try {
            parser = jsonFactory.createJsonParser(data);
            generator = jsonFactory.createJsonGenerator(baos);
            JsonToken token = null;
            while ((token = parser.nextToken()) != null) {
                switch (token) {
                case FIELD_NAME:
                    String fieldName = parser.getCurrentName();
                    generator.writeFieldName(fieldName);
                    if ("histogram_type".equals(fieldName)) {
                        parser.nextToken();
                        generator.copyCurrentEvent(parser);
                        generator.writeStringField("histogram_name", parser.getParsingContext().getParent().getCurrentName());
                    }
                    break;
                case START_OBJECT:
                    generator.writeStartObject();
                    break;
                case END_OBJECT:
                    generator.writeEndObject();
                    break;
                case START_ARRAY:
                    generator.writeStartArray();
                    break;
                case END_ARRAY:
                    generator.writeEndArray();
                    break;
                default:
                    generator.copyCurrentEvent(parser);
                    if ("ver".equals(parser.getCurrentName())) {
                        generator.writeStringField("date", date);
                    }
                    break;
                }
            }
        } finally {
            if (parser != null) {
                parser.close();
            }
            if (generator != null) {
                generator.close();
            }
        }
        
        return baos.toByteArray();
    }
    
    public void indexHBaseData(Calendar startCal, Calendar endCal, String startDate, int resumeAfterNum) {
        LOG.info("Entering indexing phase ...");
        HTableInterface table = hbasePool.getTable(tableName);
        Map<byte[],byte[]> columns = new HashMap<byte[], byte[]>();
        columns.put(columnFamily, columnQualifier);
        Scan[] scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, "yyyyMMdd", columns, BATCH_SIZE, false);
        int counter = 0;
        try {
            ResultScanner scanner = null;
            for (Scan s : scans) {
                try {
                    scanner = table.getScanner(s);
                    long prevTime = System.currentTimeMillis();
                    long curTime = System.currentTimeMillis();
                    Result[] results = null;
                    while ((results = scanner.next(BATCH_SIZE)).length > 0) {
                        if (resumeAfterNum != -1 && counter < resumeAfterNum) {
                            counter += results.length;
                            continue;
                        }
                        
                        BulkRequestBuilder brb = client.prepareBulk();
                        for (Result r : results) {
                            // Pull the date string out of the rowId into a separate field
                            String rowId = new String(r.getRow());
                            String date = rowId.substring(1,9);
                            if (!startDate.equals(date)) {
                                LOG.info("Skipping because date is: " + date);
                                continue;
                            }
                            brb.add(Requests.indexRequest(indexName).type(type).id(rowId.substring(9)).source(modifyJSON(date, r.getValue(columnFamily, columnQualifier))));
                        }

                        curTime = System.currentTimeMillis();
                        LOG.info(String.format("Scanned hbase in roughly: %d ms",(curTime-prevTime)));
                        int numActions = brb.numberOfActions();
                        LOG.info("Indexing batch size: " + numActions);
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
                        prevTime = System.currentTimeMillis();                        
                    }
                } finally {
                    if (scanner != null) {
                        scanner.close();
                    }
                }
            }

        } catch (IOException e) {
            LOG.error("IO error occurred during indexing", e);
        } finally {
            if (hbasePool != null && table != null) {
                hbasePool.putTable(table);
            }
        }
        
        LOG.info("Finished. Total documents indexed: " + counter);
    }

    public static void main(String[] args) throws IOException, ParseException {
        // Setup the start and stop dates for scanning
        Calendar startCal = Calendar.getInstance();
        Calendar endCal = Calendar.getInstance();
        if (args.length >= 2) {
            String startDateStr = args[0];
            String endDateStr = args[1];
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            startCal.setTime(sdf.parse(startDateStr));
            endCal.setTime(sdf.parse(endDateStr));
        }

        int resumeAfterNum = -1;
        if (args.length == 3) {
            resumeAfterNum = Integer.parseInt(args[2]);
        }
        
        ElasticSearchIndexer esi = null;
        try {
            esi = new ElasticSearchIndexer("telemetry_"+args[0], "data", "telemetry", "data", "json");
            esi.prepareIndexForBulk();
            esi.indexHBaseData(startCal, endCal, args[0], resumeAfterNum);
            esi.prepareIndexForQuery();
        } finally {
            if (esi != null) {
                esi.close();
            }
        }
    }

}