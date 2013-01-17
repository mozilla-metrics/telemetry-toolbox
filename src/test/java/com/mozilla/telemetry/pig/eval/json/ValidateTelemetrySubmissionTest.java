package com.mozilla.telemetry.pig.eval.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.junit.Before;
import org.junit.Test;

import com.mozilla.telemetry.constants.TelemetryConstants;

public class ValidateTelemetrySubmissionTest {
    
    private String testResourcesDir;
    private String telemetrySpecLookup;
    private ValidateTelemetrySubmissionLocal validateTelemetrySubmission;
    private ObjectMapper jsonMapper;
    
    public static class ValidateTelemetrySubmissionLocal extends ValidateTelemetrySubmission {
        public ValidateTelemetrySubmissionLocal(String fileName) {
            super(fileName);
        }
        
        protected DataInputStream getFile(String fileName) throws IOException {
            return new DataInputStream(new FileInputStream(fileName));
        }

        protected void readLookupFile() {
            DataInputStream dis = null;
            try {
                Properties specProperties = new Properties();
                specValues = new HashMap<String,Object>();
                dis = getFile(lookupFilename);
                specProperties.load(dis);
                for(String key: specProperties.stringPropertyNames()) {
                    String specFile = specProperties.getProperty(key);
                    Map<String, Map<String,Object>> referenceJson = readReferenceJson(specFile);
                    specValues.put(key, referenceJson);
                }
            } catch(IOException e) {
                LOG.info("ERROR: failed to process telemetry spec lookup file "+e.getMessage());
            } catch(Exception e) {
                LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
            } finally {
                if (dis != null) {
                    try {
                        dis.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        }

        @SuppressWarnings("unchecked")    
        protected Map<String, Map<String,Object>> readReferenceJson(String filename) {
            DataInputStream dis = null;
            try {
                Map<String, Map<String,Object>> referenceValues = new HashMap<String, Map<String,Object>>();
                ObjectMapper jsonMapper = new ObjectMapper();
                Map<String, Object> referenceJson = new LinkedHashMap<String, Object>();
                dis = getFile(filename);
                referenceJson = jsonMapper.readValue(dis, new TypeReference<Map<String,Object>>() { });
                LinkedHashMap<String, Object> histograms = (LinkedHashMap<String, Object>) referenceJson.get(TelemetryConstants.HISTOGRAMS);

                for(Map.Entry<String, Object> histogram : histograms.entrySet()) {
                    Map<String, Object> compKey = new HashMap<String, Object>();
                    String jKey = histogram.getKey();
                    LinkedHashMap<String, Object> histogram_values = (LinkedHashMap<String, Object>) histogram.getValue();
                    compKey.put(TelemetryConstants.NAME, jKey);
                    if(histogram_values.containsKey(TelemetryConstants.MIN)) {
                        compKey.put(TelemetryConstants.MIN, String.valueOf(histogram_values.get(TelemetryConstants.MIN)));
                    }
                    if(histogram_values.containsKey(TelemetryConstants.MAX)) {
                        compKey.put(TelemetryConstants.MAX, String.valueOf(histogram_values.get(TelemetryConstants.MAX)));
                    }
                    if(histogram_values.containsKey(TelemetryConstants.KIND)) {
                        compKey.put(TelemetryConstants.HISTOGRAM_TYPE, String.valueOf(histogram_values.get(TelemetryConstants.KIND)));
                    }
                    if(histogram_values.containsKey(TelemetryConstants.BUCKET_COUNT)) {
                        compKey.put(TelemetryConstants.BUCKET_COUNT, String.valueOf(histogram_values.get(TelemetryConstants.BUCKET_COUNT)));
                    }
                    if(histogram_values.containsKey(TelemetryConstants.BUCKETS)) {
                        compKey.put(TelemetryConstants.BUCKETS, (List<Integer>)histogram_values.get(TelemetryConstants.BUCKETS));
                    }
                    referenceValues.put(jKey, compKey);
                }
                return referenceValues;

            } catch (IOException e) {
                LOG.info("ERROR: failed to process telemetry spec jsons "+e.getMessage());
            } finally {
                if (dis != null) {
                    try {
                        dis.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
            
            return null;
        }

    }
    
    @Before
    public void setup() throws IOException {
        testResourcesDir = System.getProperty("basedir")+"/src/test/resources/telemetry/";
        telemetrySpecLookup = "telemetry_spec_lookup.properties";
        jsonMapper = new ObjectMapper();
        Properties prop = new Properties();
        FileOutputStream fos = null;
        try {
            prop.setProperty("default",testResourcesDir+"telemetry_spec_16.json");
            prop.setProperty("16",testResourcesDir+"telemetry_spec_16.json");
            prop.setProperty("17",testResourcesDir+"telemetry_spec_18.json");
            prop.setProperty("18",testResourcesDir+"telemetry_spec_18.json");
            prop.setProperty("19",testResourcesDir+"telemetry_spec_18.json");
            fos = new FileOutputStream(testResourcesDir+telemetrySpecLookup);
            prop.store(fos, null);
            validateTelemetrySubmission = new ValidateTelemetrySubmissionLocal(testResourcesDir+telemetrySpecLookup);
        } catch (IOException e) {
        } finally {
            if (fos != null) {
                fos.close();
            }
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
            String validatedJson = validateTelemetrySubmission.validateTelemetryJson(json);
            assertNotNull(validatedJson);
            Map<String, Object> crash = jsonMapper.readValue(validatedJson, new TypeReference<Map<String,Object>>() { });
            LinkedHashMap<String,Object> info = (LinkedHashMap<String, Object>) crash.get(TelemetryConstants.INFO);
            assertEquals(info.get(TelemetryConstants.VALID_FOR_SCHEMA),"true");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGetAppVersionFromTelemetryDoc() {
        try {
            validateTelemetrySubmission.readLookupFile();
            String json = readFile(testResourcesDir+"telemetry_test_doc");
            assertNotNull(json);
            Map<String, Object> crash = jsonMapper.readValue(json, new TypeReference<Map<String,Object>>() { });
            String version = validateTelemetrySubmission.getAppVersionFromTelemetryDoc(crash);
            assertEquals(version,"16.0");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    
}
