package excutor.impl;

import excutor.Scan;
import index.Index;
import query.Constant;

public class IndexJoinScan implements Scan {
    private Scan s;
    private Index idx;
    private String joinField;
    private TableScan ts;

    public IndexJoinScan(Scan s, Index idx, String joinField, TableScan ts){
        this.s =s ;
        this.idx = idx;
        this.joinField = joinField;
        this.ts = ts;
        StartPtr();
    }

    @Override
    public void StartPtr() {
        s.StartPtr();
        s.nextPtr();
        resetIndex();
    }

    private void resetIndex() {
        Constant searchKey = s.getVal(joinField);
        idx.startPtr(searchKey);
    }

    @Override
    public boolean nextPtr() {
        while (true) {
            if (idx.nextPtr()) {
                ts.moveToRid(idx.getDataRId());
                return true;
            }
            if (!s.nextPtr()) {
                return false;
            }
            resetIndex();
        }
    }

    @Override
    public int getAsInt(String field) {
        if(ts.hasField(field)){
            return ts.getAsInt(field);
        }else{
            return s.getAsInt(field);
        }
    }

    @Override
    public String getAsString(String field) {
        if(ts.hasField(field)){
            return ts.getAsString(field);
        }else{
            return s.getAsString(field);
        }
    }

    @Override
    public Constant getVal(String field) {
        if(ts.hasField(field)){
            return ts.getVal(field);
        }else{
            return s.getVal(field);
        }
    }

    @Override
    public boolean hasField(String field) {
        return ts.hasField(field) || s.hasField(field);
    }

    @Override
    public void close() {
        s.close();
        idx.close();
        ts.close();
    }
}
