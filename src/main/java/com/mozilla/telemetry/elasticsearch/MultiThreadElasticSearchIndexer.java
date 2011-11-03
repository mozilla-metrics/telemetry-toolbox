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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
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
public class MultiThreadElasticSearchIndexer {

    private static final Logger LOG = Logger.getLogger(MultiThreadElasticSearchIndexer.class);

    private static final int MAX_THREADS = 4;
    
    private final String indexName;
    private final String tableName;
    private final String columnFamily;
    private final String columnQualifier;

    private ExecutorService pool;
    private Semaphore sem;
    
    private Node node;
    private Client client;
    private AdminClient adminClient;
    private HTablePool hbasePool;
    
    public MultiThreadElasticSearchIndexer(String indexName, String tableName, String columnFamily, String columnQualifier) {
        this.node = nodeBuilder().loadConfigSettings(true).client(true).node();
        this.client = node.client();
        this.indexName = indexName;
        
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.regionserver.lease.period", String.valueOf(600000));
        this.hbasePool = new HTablePool(conf, 32);
        
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
        
        this.pool = Executors.newFixedThreadPool(MAX_THREADS);
        this.sem = new Semaphore(MAX_THREADS*2);
        
        // Make sure to have the index available.
        adminClient = client.admin();
        if (!adminClient.indices().exists(new IndicesExistsRequest(indexName)).actionGet().exists()) {
            LOG.info(String.format("Creating missing index '%s'", indexName));
            
            Settings settings = ImmutableSettings.settingsBuilder().put("index.number_of_replicas", 0).build();
            adminClient.indices().create(new CreateIndexRequest(indexName).settings(settings)).actionGet();
        }
        LOG.info(String.format("Waiting for index '%s'...", indexName));
        adminClient.cluster().health(new ClusterHealthRequest(indexName).waitForNodes("1")).actionGet();
        
        // Set refresh interval to -1
        Settings settings = ImmutableSettings.settingsBuilder().put("index.refresh_interval", "-1").build();
        adminClient.indices().prepareUpdateSettings(indexName).setSettings(settings).execute().actionGet();
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
    
    public void indexHBaseData(Calendar startCal, Calendar endCal) throws InterruptedException, ExecutionException {
        LOG.info("Entering indexing phase ...");

        // Init scanners to give to the worker threads
        Map<byte[],byte[]> columns = new HashMap<byte[], byte[]>();
        columns.put(columnFamily.getBytes(), columnQualifier.getBytes());
        Scan[] scans = MultiScanTableMapReduceUtil.generateBytePrefixScans(startCal, endCal, "yyyyMMdd", columns, 200, false);
        
        //List<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
        for (Scan s : scans) {
            sem.acquire();
            Future<Integer>f = pool.submit(new IndexWorker(sem, client, indexName, hbasePool, tableName, columnFamily, columnQualifier, s));
            futures.add(f);
            //tasks.add(new IndexWorker(client, indexName, hbasePool, tableName, columnFamily, columnQualifier, s));
        }
        
        int sum = 0;
        for (Future<Integer> f : futures) {
            int count = f.get();
            sum += count;
            LOG.info("Thread finished: " + (count > 0 ? "success" : "failed"));
            if (count <= 0) {
                Thread.currentThread().interrupt();
            }
        }
        
        LOG.info(String.format("Indexed %d documents", sum));
    }
    
    public void close() {
        try {
            if (pool != null) {
                try {
                    pool.shutdown();
                    while (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                        LOG.warn("Waited 30 seconds and pool still isn't shutdown");
                    }
                } catch (InterruptedException e) {
                    pool.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            if (hbasePool != null && tableName != null) {
                hbasePool.closeTablePool(tableName);
            }
            if (client != null) {
                client.close();
            }
            if (node != null) {
                node.close();
            }
        }
    }
    
    private class IndexWorker implements Callable<Integer> {

        private static final int BATCH_SIZE = 200;

        private final JsonFactory jsonFactory = new JsonFactory();
        
        private Semaphore sem;
        private Client client;
        private final String indexName;
        private final String type;
        
        private final String tableName;
        private final byte[] columnFamily;
        private final byte[] columnQualifier;
        
        private HTablePool hbasePool;
        private Scan scan;

        public IndexWorker(Semaphore sem, Client client, String indexName, HTablePool hbasePool, String tableName, String columnFamily, String columnQualifier, Scan scan) {
            this.sem = sem;
            this.client = client;
            this.indexName = indexName;
            this.type = columnFamily;
            
            this.hbasePool = hbasePool;
            this.tableName = tableName;
            this.columnFamily = Bytes.toBytes(columnFamily);
            this.columnQualifier = Bytes.toBytes(columnQualifier);
            this.scan = scan;
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
        
        @Override
        public Integer call() throws Exception {
            int counter = 0;

            HTableInterface table = null;
            ResultScanner scanner = null;
            try {                
                table = hbasePool.getTable(this.tableName);
                scanner = table.getScanner(scan);
                Result[] results = null;
                while ((results = scanner.next(BATCH_SIZE)).length > 0) {
                    BulkRequestBuilder brb = client.prepareBulk();
                    for (Result r : results) {
                        // Pull the date string out of the rowId into a separate field
                        String rowId = new String(r.getRow());
                        brb.add(Requests.indexRequest(indexName).type(type).id(rowId.substring(9)).source(modifyJSON(rowId.substring(1,9), r.getValue(columnFamily, columnQualifier))));
                    }

                    int numActions = brb.numberOfActions();
                    String threadName = Thread.currentThread().getName();
                    LOG.info(String.format("%s - Indexing batch of size %d", threadName, numActions));
                    BulkResponse response = brb.execute().actionGet();
                    LOG.info(String.format("%s - Bulk request finished in %dms", threadName, response.getTookInMillis()));
                    if (response.hasFailures()) {
                        LOG.error(String.format("%s - Had failures during bulk indexing", threadName));
                        counter = -1;
                        break;
                    }
                    counter += numActions;
                    LOG.info(String.format("%s - Indexed %d documents so far", threadName, counter));
                }
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
                if (hbasePool != null && table != null) {
                    hbasePool.putTable(table);
                }
                sem.release();
            }
            
            return counter;
        }
        
    }
    
    public static void main(String[] args) throws InterruptedException, ExecutionException, ParseException {
        // Setup the start and stop dates for scanning
        Calendar startCal = Calendar.getInstance();
        Calendar endCal = Calendar.getInstance();
        if (args.length == 2) {
            String startDateStr = args[0];
            String endDateStr = args[1];
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
            startCal.setTime(sdf.parse(startDateStr));
            endCal.setTime(sdf.parse(endDateStr));
        }

        MultiThreadElasticSearchIndexer esi = null;
        try {
            esi = new MultiThreadElasticSearchIndexer("telemetry_" + args[0], "telemetry", "data", "json");
            esi.prepareIndexForBulk();
            esi.indexHBaseData(startCal, endCal);
            esi.prepareIndexForQuery();
        } finally {
            if (esi != null) {
                esi.close();
            }
        }
    }

}
