package plan.Impl;

import excutor.UpdateScan;
import index.Index;
import metadata.MetaIndex;
import metadata.mgr.MetaMgr;
import mvcc.Transaction;
import parse.*;
import plan.Plan;
import plan.UpdatePlanner;
import query.Constant;
import record.RId;

import java.util.Iterator;
import java.util.Map;

/**
 * 基础updatePlanner的修正，它配发了每一个update语句到相关的index planner去
 */
public class IndexUpdatePlanner implements UpdatePlanner {
    private MetaMgr mm;
    private IndexUpdatePlanner(MetaMgr mm){
        this.mm = mm;
    }

    @Override
    public int executeInsert(InsertData data, Transaction tx) {
        String tblname = data.getTblname();
        Plan p = new TablePlan(tx, tblname, mm);

        UpdateScan us = (UpdateScan) p.open();
        us.insert();
        RId rid = us.getRid();

        Map<String, MetaIndex> indexMap = mm.getIndexInfo(tblname, tx);
        Iterator<Constant> valIter = data.getVals().iterator();
        for(String fldname:data.getFlds()){
            Constant val = valIter.next();
            us.setConstant(fldname, val);

            MetaIndex mi = indexMap.get(fldname);
            if(mi != null){
                Index idx = mi.open();
                idx.insert(val, rid);
                idx.close();
            }
        }
        us.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction tx) {
        String tblname = data.getTblname();
        Plan p = new TablePlan(tx, tblname, mm);
        p = new SelectPlan(p, data.getPred());
        Map<String, MetaIndex> indexMap = mm.getIndexInfo(tblname, tx);

        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.nextPtr()){
            RId rid = us.getRid();
            for(String fldname : indexMap.keySet()){
                Constant val = us.getVal(fldname);
                Index idx = indexMap.get(fldname).open();
                idx.delete(val, rid);
                idx.close();
            }
            us.delete();
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeModify(ModifyData data, Transaction tx) {
        String tblname = data.getTblname();
        String fldname = data.getFldname();
        Plan p = new TablePlan(tx, tblname, mm);
        p = new SelectPlan(p, data.getPred());

        MetaIndex mi = mm.getIndexInfo(tblname, tx).get(fldname);
        Index idx = (mi == null) ? null : mi.open();

        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while (us.nextPtr()){
            // first, update the simpledb.record
            Constant newval = data.getNewval().evaluate(us);
            Constant oldval = us.getVal(fldname);
            us.setConstant(data.getFldname(), newval);

            // then update the appropriate simpledb.index, if it exists
            if (idx != null) {
                RId rid = us.getRid();
                idx.delete(oldval, rid);
                idx.insert(newval, rid);
            }
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeCreateTable(CreateTable data, Transaction tx) {
        mm.createTable(data.getTblname(), data.getSch(), tx);
        return 0;
    }

    @Override
    public int executeCreateView(CreateView data, Transaction tx) {
        mm.createView(data.getViewname(), data.getViewDef(), tx);
        return 0;
    }

    @Override
    public int executeCreateIndex(CreateIndex data, Transaction tx) {
        mm.createIndex(data.getIdxname(), data.getTblname(), data.getFldname(), tx);
        return 0;
    }
}
