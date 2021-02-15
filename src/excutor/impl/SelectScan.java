package excutor.impl;

import excutor.Scan;
import excutor.UpdateScan;
import query.Constant;
import query.Predicate;
import record.RId;

public class SelectScan implements UpdateScan {
    private Scan s;
    private Predicate pred;

    public SelectScan(Scan s, Predicate pred){
        this.s = s;
        this.pred = pred;
    }

    @Override
    public void setInt(String field, int val) {
        UpdateScan us = (UpdateScan) s;
        us.setInt(field, val);
    }

    @Override
    public void setString(String field, String val) {
        UpdateScan us = (UpdateScan) s;
        us.setString(field, val);
    }

    @Override
    public void setConstant(String field, Constant val) {
        UpdateScan us = (UpdateScan) s;
        us.setConstant(field, val);
    }

    @Override
    public void insert() {
        UpdateScan us = (UpdateScan) s;
        us.insert();
    }

    @Override
    public void delete() {
        UpdateScan us = (UpdateScan) s;
        us.delete();
    }

    @Override
    public RId getRid() {
        UpdateScan us = (UpdateScan) s;
        return us.getRid();
    }

    @Override
    public void moveToRid(RId rid) {
        UpdateScan us = (UpdateScan) s;
        us.moveToRid(rid);
    }

    @Override
    public void StartPtr() {
        s.StartPtr();
    }

    @Override
    public boolean nextPtr() {
        while (s.nextPtr()) {
            if (pred.holds(s)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getAsInt(String field) {
        return s.getAsInt(field);
    }

    @Override
    public String getAsString(String field) {
        return s.getAsString(field);
    }

    @Override
    public Constant getVal(String field) {
        return s.getVal(field);
    }

    @Override
    public boolean hasField(String field) {
        return s.hasField(field);
    }

    @Override
    public void close() {
        s.close();
    }
}
