package excutor.impl;

import excutor.Scan;
import plan.AggregationFn;
import query.Constant;
import query.GroupValue;

import java.util.List;

public class GroupByScan implements Scan {
    private Scan s;
    private List<String> groupfields;
    private List<AggregationFn> aggfns;
    private GroupValue groupValue;
    private boolean moregroups;
    private AggregationFn fn;

    public GroupByScan(Scan s, List<String> groupfields, List<AggregationFn> aggfns){
        this.s = s;
        this.groupfields = groupfields;
        this.aggfns = aggfns;
        StartPtr();
    }


    @Override
    public void StartPtr() {
        s.StartPtr();
        moregroups = s.nextPtr();
    }

    @Override
    public boolean nextPtr() {
        if(!moregroups){
            return false;
        }
        for(AggregationFn fn : aggfns){
            fn.processFirst(s);
        }
        groupValue = new GroupValue(s, groupfields);
        while(moregroups = s.nextPtr()){
            GroupValue gv = new GroupValue(s, groupfields);
            if(!groupValue.equals(gv)){
                break;
            }
            for(AggregationFn fn: aggfns){
                fn.processNext(s);
            }
        }
        return true;
    }

    @Override
    public int getAsInt(String field) {
        return getVal(field).asInt();
    }

    @Override
    public String getAsString(String field) {
        return getVal(field).asString();
    }

    @Override
    public Constant getVal(String field) {
        if (groupfields.contains(field)){
            return groupValue.getVal(field);
        }
        for(AggregationFn fn : aggfns){
            if(fn.fieldName().equals(field)){
                return fn.value();
            }
        }
        throw new RuntimeException("field" + field + " not found.");
    }



    @Override
    public boolean hasField(String field) {
        if (groupfields.contains(field))
            return true;
        for (AggregationFn fn : aggfns)
            if (fn.fieldName().equals(field))
                return true;
        return false;
    }

    @Override
    public void close() {
        s.close();
    }
}
