package com.mozilla.telemetry.pig.eval.json;

import java.io.IOException;
import java.util.Map;
import java.math.BigInteger;
import java.security.*;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;
import org.apache.log4j.Logger;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class TelemetryValueCompare extends FilterFunc {
    static final Logger LOG = Logger.getLogger(TelemetryValueCompare.class);
    private ObjectMapper jsonMapper;
    private String jsonKey;
    private String subJsonKey;
    private String comparator;
    private Integer compareValue;

    public TelemetryValueCompare(String jsonKey,String comparator,String compareValue) {
        jsonMapper = new ObjectMapper();
        this.jsonKey = jsonKey;
        this.comparator = comparator;
        this.compareValue = Integer.parseInt(compareValue);
    }

    public TelemetryValueCompare(String jsonKey,String subJsonKey,String comparator,String compareValue) {
        jsonMapper = new ObjectMapper();
        this.jsonKey = jsonKey;
        this.subJsonKey = subJsonKey;
        this.comparator = comparator;
        this.compareValue = Integer.parseInt(compareValue);
    }

    @Override
    public Boolean exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        String json = (String) input.get(0);
        return getValueFromJson(json);
    }


    @SuppressWarnings("unchecked")
    protected Boolean getValueFromJson(String json) {
        try {
            Map<String, Object> jsonMap = jsonMapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            Map<String, Object> histograms = (Map<String,Object>) jsonMap.get("histograms");
            Map<String, Object> simpleMeasurements = (Map<String,Object>) jsonMap.get("simpleMeasurements");

            if (histograms.containsKey(jsonKey)) {
                Map<String,Object> histogram  = (Map<String,Object>) histograms.get(jsonKey);
                if (subJsonKey == null || subJsonKey.equals("values")) {
                    Map<String,Object> histValues = (Map<String,Object>)histogram.get("values");
                    return compareJsonMap(histValues);
                } else {
                    Integer jsonValue = (Integer) histogram.get(subJsonKey);
                    return compareJsonValue(jsonValue);
                }
            } else if (simpleMeasurements.containsKey(jsonKey)) {
                Integer jsonValue = (Integer) simpleMeasurements.get(jsonKey);
                return compareJsonValue(jsonValue);
            }
            return false;
        } catch(Exception e) {
            LOG.error(e);
        }
        return false;
    }

    protected Boolean compareJsonMap(Map<String,Object>values) {
        Boolean cmpFlag = false;
        Integer histValue = null;
        for (String key: values.keySet()) {
            histValue = Integer.parseInt(key);
            cmpFlag = compareJsonValue(histValue);
            if (cmpFlag)
                return cmpFlag;
        }
        return cmpFlag;
    }
    
    protected Boolean compareJsonValue(Integer jsonValue) {
        if (comparator.equals(">"))
            return jsonValue > compareValue;
        else if (comparator.equals("<"))
            return jsonValue < compareValue;
        else if (comparator.equals("="))
            return jsonValue == compareValue;
        else if (comparator.equals("<="))
            return jsonValue <= compareValue;
        else if (comparator.equals(">="))
            return jsonValue >= compareValue;
        return false;
    }

}
