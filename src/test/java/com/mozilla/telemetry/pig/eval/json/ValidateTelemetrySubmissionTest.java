package com.mozilla.telemetry.pig.eval.json;

import static org.junit.Assert.*;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.FuncSpec;

import org.junit.Test;
import org.junit.Before;
import org.junit.AfterClass;

import com.mozilla.telemetry.constants.TelemetryConstants;

public class ValidateTelemetrySubmissionTest {
    
    private String testResourcesDir;
    private String telemetrySpecLookup;
    private String telemetryData;
    private ValidateTelemetrySubmissionLocal validateTelemetrySubmission;
    
    public static class ValidateTelemetrySubmissionLocal extends ValidateTelemetrySubmission {
        public ValidateTelemetrySubmissionLocal(String fileName) {
            super(fileName);
        }
        
        protected DataInputStream getFile(String fileName) throws IOException {
            return new DataInputStream(new FileInputStream(fileName));
        }

        protected void readLookupFile() {
            try {
                Properties telemetry_spec_properties = new Properties();
                specValues = new HashMap<String,Object>();
                DataInputStream fi = getFile(lookupFilename);
                telemetry_spec_properties.load(fi);
                for(String key: telemetry_spec_properties.stringPropertyNames()) {
                    String spec_file = telemetry_spec_properties.getProperty(key);
                    Map<String, Map<String,Object>> referenceJson = readReferenceJson(spec_file);
                    specValues.put(key,referenceJson);
                }
            } catch(IOException e) {
                LOG.info("ERROR: failed to process telemetry spec lookup file "+e.getMessage());
            } catch(Exception e) {
                LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
            }
        }

        @SuppressWarnings("unchecked")    
        protected Map<String, Map<String,Object>>  readReferenceJson(String filename)  {
            try {
                Map<String, Map<String,Object>> referenceValues = new HashMap<String, Map<String,Object>>();
                ObjectMapper jsonMapper = new ObjectMapper();
                Map<String, Object> referenceJson = new LinkedHashMap<String, Object>();
                DataInputStream fi = getFile(filename);
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

    }
    
    @Before
    public void setup() {
        testResourcesDir = System.getProperty("basedir")+"/src/test/resources/telemetry/";
        telemetrySpecLookup = "telemetry_spec_lookup.properties";
        telemetryData = "telemetry_test_doc";
        Properties prop = new Properties();
        try {
            prop.setProperty("default",testResourcesDir+"telemetry_spec_16.json");
            prop.setProperty("16",testResourcesDir+"telemetry_spec_16.json");
            prop.setProperty("17",testResourcesDir+"telemetry_spec_18.json");
            prop.setProperty("18",testResourcesDir+"telemetry_spec_18.json");
            prop.setProperty("19",testResourcesDir+"telemetry_spec_18.json");
            prop.store(new FileOutputStream(testResourcesDir+telemetrySpecLookup),null);
            validateTelemetrySubmission =
                new ValidateTelemetrySubmissionLocal(testResourcesDir+telemetrySpecLookup);
        }catch (IOException e) {
        }
    }

    private String readFile(String path) throws IOException {
        BufferedReader reader = null;
        StringBuilder sb = new StringBuilder();
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        
        return sb.toString();
    }

    @Test
    public void testReadLookupFile()  {
        try {
            validateTelemetrySubmission.readLookupFile();
            assertEquals(validateTelemetrySubmission.specValues.size(),5);
        } catch(Exception e ) {
            fail(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testValidateTelemetryJson() {
        try {
            validateTelemetrySubmission.readLookupFile();
            String json = readFile(testResourcesDir+"telemetry_test_doc");
            assertNotNull(json);
            String validated_json = validateTelemetrySubmission.validateTelemetryJson(json);
            assertNotNull(validated_json);
            ObjectMapper jsonMapper = new ObjectMapper();
            Map<String, Object> crash = jsonMapper.readValue(validated_json, new TypeReference<Map<String,Object>>() { });
            LinkedHashMap<String,Object> info = (LinkedHashMap<String, Object>) crash.get(TelemetryConstants.INFO);
            assertEquals(info.get(TelemetryConstants.VALID_FOR_SCHEMA),"true");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetAppVersionFromTelemetryDoc() {
        try {
            validateTelemetrySubmission.readLookupFile();
            String json = readFile(testResourcesDir+"telemetry_test_doc");
            assertNotNull(json);
            ObjectMapper jsonMapper = new ObjectMapper();
            Map<String, Object> crash = jsonMapper.readValue(json, new TypeReference<Map<String,Object>>() { });
            String version = validateTelemetrySubmission.getAppVersionFromTelemetryDoc(crash);
            assertEquals(version,"16.0");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    
}
