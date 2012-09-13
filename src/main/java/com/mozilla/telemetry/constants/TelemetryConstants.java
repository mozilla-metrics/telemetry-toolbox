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
package com.mozilla.telemetry.constants;

public interface TelemetryConstants {

    public static final String TELEMETRY_INVALID_COUNTS = "TelemetryInvalidCounts";
    
    //public static final String HBASE_HOST = "admin1.research.hadoop.sjc1.mozilla.com";
    //public static final String HBASE_HOST = "hp-node62.phx1.mozilla.com";

    // HBase table and column names
    public static final String TABLE_NAME = "telemetry";
    public static String startDate = "";
    public static String endDate = "";

    public static final byte[] PROCESSED_DATA_BYTES = "data".getBytes();
    public static final byte[] META_DATA_BYTES = "meta_data".getBytes();
    public static final byte[] RAW_DATA_BYTES = "raw_data".getBytes();

    public static final byte[] JSON_BYTES = "json".getBytes();

    // Configuration fields
    public static final String PRODUCT_FILTER = "product.filter";
    public static final String PRODUCT_VERSION = "product.version";
    public static final String DLL_VERSION = "dll.version";

    public static final String RELEASE_FILTER = "release.filter";
    public static final String CONDENSE = "condense";
    public static final String GROUP_BY_OS_VERSION = "group.by.os.version";
    public static final String START_DATE = "start.date";
    public static final String END_DATE = "end.date";
    public static final String START_TIME = "start.time";
    public static final String END_TIME = "end.time";

    // Crash JSON fields
    public static final String PRODUCT = "product";
    public static final String VERSION = "version";
    public static final String OS_NAME = "os_name";
    public static final String OS_VERSION = "os_version";
    public static final String LINUX = "Linux";
    public static final String SIGNATURE = "signature";
    public static final String REASON = "reason";
    public static final String DUMP_COLUMN = "dump";
    public static final String DATA = "data";
    public static final String PROCESSED_DATA = "processed_data";
    public static final String JSON_COLUMN = "json";
    public static final String APP_VERSION = "appVersion";
    public static final String FIREFOX_VERSION = "15";

    public static final String KIND = "kind";
    public static final String VER = "ver";
    public static final String INFO = "info";
    public static final String SIMPLE_MESAUREMENTS = "simpleMesaurements";
    public static final String HISTORGRAMS = "histograms";
    public static final String NAME = "name";
    public static final String RANGE = "range";
    public static final String MIN = "min";
    public static final String MAX = "max";
    public static final String VALUES = "values";
    public static final String INVALID_VALUES = "invalid_values";
    public static final String MISSING_JSON_REFERENCE = "MISSING_JSON_REFERENCE";
    public static final String HISTOGRAMS = "histograms";
    public static final String HISTOGRAM_TYPE = "histogram_type";
    public static final String EXPONENTIAL = "exponential";
    public static final String LINEAR = "linear";
    public static final String BOOLEAN = "boolean";
    public static final String ENUMERATED = "enumerated";
    public static final String FLAG = "flag";
    public static final String BUCKET_COUNT = "bucket_count";
    public static final String BUCKETS = "buckets";
    
    public static final String RAW_DATA = "raw_data";

    public static final String DATE_PROCESSED = "date_processed";
    public static final String CPU_PATTERN = "CPU|";
    public static final String CLIENT_CRASH_DATE = "client_crash_date";
    public static final String BUILD = "build";
    public static final String URL = "URL";
    public static final String baseUrl = "https://crash-stats.mozilla.com/report/index/";

    public static final String KEY_DELIMITER = "\u0001";
    public static final String CORE_INFO_DELIMITER = "\u0002";
    public static final String COMPKEY_DELIMITER = ":";

}
