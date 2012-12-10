/**
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
package com.mozilla.telemetry.pig.eval;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class KernelToAndroidVersion extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        String androidVersion = "Unknown";
        String kernelVersion = (String)input.get(0);
        if (kernelVersion.startsWith("2.6.27")) {
            androidVersion = "1.5";
        } else if (kernelVersion.startsWith("2.6.29")) {
            androidVersion = "1.6/2.0.x/2.1";
        } else if (kernelVersion.startsWith("2.6.32")) {
            androidVersion = "2.2.x";
        } else if (kernelVersion.startsWith("2.6.35")) {
            androidVersion = "2.3.x";
        } else if (kernelVersion.startsWith("2.6.36")) {
            androidVersion = "3.x";
        } else if (kernelVersion.startsWith("2.6.39.4") || kernelVersion.startsWith("3.0.1") || 
                   kernelVersion.startsWith("3.0.8")) {
            androidVersion = "4.0.x";
        } else if (kernelVersion.startsWith("3.0.31") || kernelVersion.startsWith("3.1.10")) {
            androidVersion = "4.1.x";
        } else if (kernelVersion.contains("Cyanogen") || kernelVersion.contains("cyanogen")) {
            androidVersion = "CyanogenMod";
        } else {
            androidVersion = "Unknown (" + kernelVersion + ")";
        }
        
        return androidVersion;
    }

}
