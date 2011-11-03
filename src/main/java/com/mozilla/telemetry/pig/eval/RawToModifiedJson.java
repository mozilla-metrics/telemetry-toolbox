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
package com.mozilla.telemetry.pig.eval;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

public class RawToModifiedJson extends EvalFunc<String>  {

    private JsonFactory jsonFactory = new JsonFactory();
    
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        String id = (String)input.get(0);
        String date = (String)input.get(1);
        String data = (String)input.get(2);
        
        StringWriter writer = new StringWriter();
        JsonParser parser = null;
        JsonGenerator generator = null;
        try {
            parser = jsonFactory.createJsonParser(data);
            generator = jsonFactory.createJsonGenerator(writer);
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
                        generator.writeStringField("id", id);
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
        
        return writer.toString();
    }
    
}
