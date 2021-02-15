package excutor.impl;

import excutor.Scan;
import index.Index;
import query.Constant;
import record.RId;

public class IndexSelectScan implements Scan {
    private TableScan ts;
    private Index idx;
    private Constant val;


    public IndexSelectScan(TableScan ts, Index idx, Constant val){
        this.ts = ts;
        this.idx = idx;
        this.val = val;
        StartPtr();
    }


    @Override
    public void StartPtr() {
        idx.startPtr(val);
    }

    @Override
    public boolean nextPtr() {
        boolean ok = idx.nextPtr();
        if (ok) {
            RId rid = idx.getDataRId();
            ts.moveToRid(rid);
        }
        return ok;
    }

    @Override
    public int getAsInt(String field) {
        return ts.getAsInt(field);
    }

    @Override
    public String getAsString(String field) {
        return ts.getAsString(field);
    }

    @Override
    public Constant getVal(String field) {
        return ts.getVal(field);
    }

    @Override
    public boolean hasField(String field) {
        return ts.hasField(field);
    }

    @Override
    public void close() {
        idx.close();
        ts.close();
    }
}
