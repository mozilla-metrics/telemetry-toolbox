package com.mozilla.telemetry.pig.eval.json;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class AddonPrivacyCorrectionTest {

    private TupleFactory tupleFactory = TupleFactory.getInstance();
    
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
    public void testExec1() throws IOException {
        AddonPrivacyCorrection apc = new AddonPrivacyCorrection();
        
        String testInputPath = System.getProperty("basedir") + "/src/test/resources/apctest1-input.js";
        String inputJson = readFile(testInputPath);
        Tuple input = tupleFactory.newTuple(inputJson);
        String output = apc.exec(input);
        assertNotNull(output);
        assertFalse(output.contains("facebook"));
        assertFalse(output.contains("twitter"));
    }
    
}
