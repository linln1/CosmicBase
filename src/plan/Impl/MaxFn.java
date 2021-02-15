package plan.Impl;

import excutor.Scan;
import plan.AggregationFn;
import query.Constant;

public class MaxFn implements AggregationFn {
    private String fld;
    private Constant val;

    public MaxFn(String fldname) {
        this.fld = fldname;
    }

    @Override
    public void processFirst(Scan s) {
        val = s.getVal(fld);
    }

    @Override
    public void processNext(Scan s) {
        Constant newval = s.getVal(fld);
        if(newval.compareTo(val) > 0){
            val = newval;
        }
    }

    @Override
    public String fieldName() {
        return "max of " + fld;
    }

    @Override
    public Constant value() {
        return val;
    }
}
