package com.mozilla.telemetry.pig.eval;

import java.io.IOException;
import java.util.Map;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.mozilla.telemetry.pig.eval.HistogramValueTuples.ERRORS;

public class SingleHistogramTuple extends EvalFunc<DataBag> {
    
    private static BagFactory bagFactory = BagFactory.getInstance();
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    
    private static final int OUTPUT_TUPLE_SIZE = 8;
    private static final int HIST_VALUE_IDX = 0;
    private static final int VALUE_COUNT_IDX = 1;
    private static final int SUM_IDX = 2;
    private static final int BUCKET_COUNT_IDX = 3;
    private static final int MIN_RANGE_IDX = 4;
    private static final int MAX_RANGE_IDX = 5;
    private static final int HIST_TYPE_IDX = 6;
    protected static final int HIST_IS_VALID = 7;
    
    @SuppressWarnings("unchecked")
    @Override
    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        
        try {
            DataBag output = bagFactory.newDefaultBag();
            Map<String,Object> hv = (Map<String,Object>)input.get(0);
            if(hv != null) {
                Map<String,Object> values = (Map<String,Object>)hv.get("values");
                // If any of these cases are true just flag the entry as bad and continue
                if (values == null || !hv.containsKey("sum") || !hv.containsKey("bucket_count") ||
                    !hv.containsKey("range") || !hv.containsKey("histogram_type") ||
                    hv.get("sum") == null || hv.get("bucket_count") == null || 
                    hv.get("range") == null || hv.get("histogram_type") == null) {
                    System.out.println("bad histogram");
                    warn("Encountered bad histogram tuple", ERRORS.BAD_HISTOGRAM_TUPLE);
                    return null;
                }
    
                long sum = ((Number)hv.get("sum")).longValue();
                int bucketCount = ((Number)hv.get("bucket_count")).intValue();
                int histogramType = ((Number)hv.get("histogram_type")).intValue();
                int isValid = 1;
                if (hv.containsKey("valid")) {
                    isValid = Boolean.parseBoolean((String)hv.get("valid")) ? 1 : 0;
                }
                int minRange = 0, maxRange = 0;
                DataBag rangeBag = (DataBag)hv.get("range");
                Iterator<Tuple> rangeIter = rangeBag.iterator();
                Tuple rangeTuple = rangeIter.next();
                if (rangeTuple.size() >= 2) {
                    if (rangeTuple.get(0) instanceof Number) {
                        minRange = ((Number)rangeTuple.get(0)).intValue();
                    }
                    if (rangeTuple.get(1) instanceof Number) {
                        maxRange = ((Number)rangeTuple.get(1)).intValue();
                    }
                }
    
                for (Map.Entry<String, Object> v : values.entrySet()) {
                    long histValue = -1;
                    try {
                        histValue = Long.parseLong(v.getKey());
                    } catch (NumberFormatException e) {
                        pigLogger.warn(this, "Non-numeric histogram value incountered", ERRORS.NON_NUMERIC_HIST_VALUE);
                        continue;
                    }
                    int value_count = ((Number)v.getValue()).intValue();
                    if (value_count == 0) continue;
                    Tuple t = tupleFactory.newTuple(OUTPUT_TUPLE_SIZE);
                    t.set(HIST_VALUE_IDX, histValue);
                    t.set(VALUE_COUNT_IDX, ((Number)v.getValue()).doubleValue());
                    t.set(SUM_IDX, sum);
                    t.set(BUCKET_COUNT_IDX, bucketCount);
                    t.set(MIN_RANGE_IDX, minRange);
                    t.set(MAX_RANGE_IDX, maxRange);
                    t.set(HIST_TYPE_IDX, histogramType);
                    t.set(HIST_IS_VALID, isValid);
                    output.add(t);
                }

            }
            
            return output;
        }catch (Exception e) {
            System.out.println(e);
            warn("Exception while processing histogram names", ERRORS.GENERIC_ERROR);
        }
        
        return null;
    }
    
}
