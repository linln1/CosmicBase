package excutor.impl;

import excutor.Scan;
import excutor.UpdateScan;
import query.Constant;
import record.RId;
import record.RecordComparator;
import record.TempTable;

import java.util.Arrays;
import java.util.List;

public class SortScan implements Scan {
    private UpdateScan s1, s2= null, currentscan = null;
    private RecordComparator comp;
    private boolean hasmore1, hasmore2 = false;
    private List<RId> savedposition;


    public SortScan(List<TempTable> runs, RecordComparator comp){
        this.comp = comp;
        s1 = (UpdateScan) runs.get(0).open();
        hasmore1 = s1.nextPtr();
        if(runs.size() > 1){
            s2 = (UpdateScan) runs.get(1).open();
            hasmore2 = s2.nextPtr();
        }
    }

    @Override
    public void StartPtr() {
        currentscan = null;
        s1.StartPtr();
        hasmore1 = s1.nextPtr();
        if(s2 != null){
            s2.StartPtr();
            hasmore2 = s2.nextPtr();
        }
    }

    @Override
    public boolean nextPtr() {
        if (currentscan != null){
            if(currentscan == s1){
                hasmore1 = s1.nextPtr();
            }
            else if (currentscan == s2){
                hasmore2 = s2.nextPtr();
            }
        }

        if (!hasmore1 && !hasmore2){
            return false;
        }else if(hasmore1 && hasmore2){
            if(comp.compare(s1, s2) < 0){
                currentscan = s1;
            }else{
                currentscan = s2;
            }
        }else if(hasmore1){
            currentscan = s1;
        }else if(hasmore2){
            currentscan = s2;
        }
        return true;
    }

    @Override
    public int getAsInt(String field) {
        return currentscan.getAsInt(field);
    }

    @Override
    public String getAsString(String field) {
        return currentscan.getAsString(field);
    }

    @Override
    public Constant getVal(String field) {
        return currentscan.getVal(field);
    }

    @Override
    public boolean hasField(String field) {
        return currentscan.hasField(field);
    }

    @Override
    public void close() {
        s1.close();
        if(s2 != null){
            s2.close();
        }
    }

    public void setSavedposition(){
        RId rid1 = s1.getRid();
        RId rid2 = (s2 == null) ? null : s2.getRid();
        savedposition = Arrays.asList(rid1, rid2);
    }

    public void restorePosition(){
        RId rid1 = savedposition.get(0);
        RId rid2 = savedposition.get(1);
        s1.moveToRid(rid1);
        if (rid2 != null) {
            s2.moveToRid(rid2);
        }
    }
}
