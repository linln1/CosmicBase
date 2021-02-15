package excutor.impl;

import excutor.Scan;
import query.Constant;

public class MergeJoinScan implements Scan {
    private Scan s1;
    private SortScan s2;
    private String fld1, fld2;
    private Constant joinval = null;

    public MergeJoinScan(Scan s1, SortScan s2, String fld1, String fld2){
        this.s1 = s1;
        this.s2 = s2;
        this.fld1 = fld1;
        this.fld2 = fld2;
        StartPtr();
    }

    @Override
    public void StartPtr() {
        s1.StartPtr();
        s2.StartPtr();
    }

    @Override
    public boolean nextPtr() {
        boolean hasmore2 = s2.nextPtr();
        if (hasmore2 && s2.getVal(fld2).equals(joinval)) {
            return true;
        }

        boolean hasmore1 = s1.nextPtr();
        if (hasmore1 && s1.getVal(fld1).equals(joinval)) {
            s2.restorePosition();
            return true;
        }

        while (hasmore1 && hasmore2) {
            Constant v1 = s1.getVal(fld1);
            Constant v2 = s2.getVal(fld2);
            if (v1.compareTo(v2) < 0) {
                hasmore1 = s1.nextPtr();
            } else if (v1.compareTo(v2) > 0) {
                hasmore2 = s2.nextPtr();
            } else {
                s2.setSavedposition();
                joinval  = s2.getVal(fld2);
                return true;
            }
        }
        return false;
    }

    @Override
    public int getAsInt(String field) {
        if (s1.hasField(field)) {
            return s1.getAsInt(field);
        } else {
            return s2.getAsInt(field);
        }
    }

    @Override
    public String getAsString(String field) {
        if (s1.hasField(field)) {
            return s1.getAsString(field);
        } else {
            return s2.getAsString(field);
        }
    }

    @Override
    public Constant getVal(String field) {
        if (s1.hasField(field)) {
            return s1.getVal(field);
        } else {
            return s2.getVal(field);
        }
    }

    @Override
    public boolean hasField(String field) {
        return s1.hasField(field) || s2.hasField(field);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }
}
