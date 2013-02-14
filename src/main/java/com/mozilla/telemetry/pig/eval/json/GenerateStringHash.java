package com.mozilla.telemetry.pig.eval.json;

import java.io.IOException;
import java.util.Map;
import java.math.BigInteger;
import java.security.*;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.log4j.Logger;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class GenerateStringHash extends EvalFunc<String> {
    
    static final Logger LOG = Logger.getLogger(GenerateStringHash.class);
    private ObjectMapper jsonMapper;
    
    public GenerateStringHash() {
        jsonMapper = new ObjectMapper();
    }
    
    @Override
    public String exec(Tuple input) throws IOException {
        try {
            String json = (String) input.get(0);
            String info = fetchInfo(json);
            byte[] bytesOfJson = info.getBytes("UTF-8");
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(bytesOfJson);
            String hash = new BigInteger(1,digest).toString(16);
            return hash;
        } catch(Exception e) {
            LOG.error(e);
        }
        return null;
    }

    protected String fetchInfo(String json) {
        try {
            Map<String, Object> jsonMap = jsonMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            Map<String, Object> info = (Map<String,Object>) jsonMap.get("info");
            StringBuffer sb = new StringBuffer();
            if (info.containsKey("reason")) 
                sb.append(info.get("reason"));
            if (info.containsKey("OS"))
                sb.append(info.get("OS"));
            if (info.containsKey("appID"))
                sb.append(info.get("appID"));
            if (info.containsKey("appBuildID"))                
                sb.append(info.get("appBuildID"));
            if (info.containsKey("appName"))
                sb.append(info.get("appName"));
            if (info.containsKey("platformBuildID"))
                sb.append(info.get("platformBuildID"));
            if (info.containsKey("appUpdateChannel"))
                sb.append(info.get("appUpdateChannel"));
            /*if (info.containsKey("platformBuildID"))
              sb.append(info.get("platformBuildID"));*/
            if (info.containsKey("locale"))
                sb.append(info.get("locale"));
            if (info.containsKey("cpucount"))
                sb.append(info.get("cpucount"));
            /*if (info.containsKey("memsize"))
              sb.append(info.get("memsize"));*/
            if (info.containsKey("arch"))
                sb.append(info.get("arch"));
            if (info.containsKey("version"))
                sb.append(info.get("version"));
            if (info.containsKey("device"))
                sb.append(info.get("device"));
            if (info.containsKey("hardware"))
                sb.append(info.get("hardware"));
            if (info.containsKey(""))
                sb.append(info.get("hardware"));
            if (info.containsKey("hasMMX"))
                sb.append(info.get("hasMMX"));
            if (info.containsKey("hasSSE"))
                sb.append(info.get("hasSSE"));
            if (info.containsKey("hasSSE2"))
                sb.append(info.get("hasSSE2"));
            if (info.containsKey("hasSSE3"))
                sb.append(info.get("hasSSE3"));
            if (info.containsKey("hasSSE4A"))
                sb.append(info.get("hasSSE4A"));
            if (info.containsKey("hasSSSE3"))
                sb.append(info.get("hasSSSE3"));
            if (info.containsKey("hasSSE4_1"))
                sb.append(info.get("hasSSE4_1"));
            if (info.containsKey("hasSSE4_2"))
                sb.append(info.get("hasSSE4_2"));
            if (info.containsKey("hasEDSP"))
                sb.append(info.get("hasEDSP"));
            if (info.containsKey("hasARMv6"))
                sb.append(info.get("hasARMv6"));
            if (info.containsKey("hasARMv7"))
                sb.append(info.get("hasARMv7"));
            if (info.containsKey("adapterDescription"))
                sb.append(info.get("adapterDescription"));
            if (info.containsKey("adapterVendorID"))
                sb.append(info.get("adapterVendorID"));
            if (info.containsKey("apapterDeviceID"))
                sb.append(info.get("adapterDeviceID"));
            if (info.containsKey("adapterRAM"))
                sb.append(info.get("adapterRAM"));
            if (info.containsKey("adapterDriver"))
                sb.append(info.get("adapterDriver"));
            if (info.containsKey("adapterDriverVersion"))
                sb.append(info.get("adapterDriverVersion"));
            if (info.containsKey("adapterDriverDate"))
                sb.append(info.get("adapterDriverDate"));
            if (info.containsKey("DWriteEnabled"))
                sb.append(info.get("DWriteEnabled"));
            if (info.containsKey("DWriteVersion"))
                sb.append(info.get("DWriteVersion"));
            if (info.containsKey("DWriteEnabled"))
                sb.append(info.get("DWriteEnabled"));
            /*if (info.containsKey("addons"))
              sb.append(info.get("addons"));*/
            if (info.containsKey("flashVersion"))
                sb.append(info.get("flashVersion"));
            
            return sb.toString();
        } catch(Exception e) {
            LOG.error(e);
        }
        return null;
    }
}
