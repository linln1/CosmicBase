package plan.Impl;

import excutor.UpdateScan;
import metadata.mgr.MetaMgr;
import mvcc.Transaction;
import parse.*;
import plan.Plan;
import query.Constant;

import java.util.Iterator;

public class UpdatePlanner implements plan.UpdatePlanner{
    private MetaMgr mm;

    public UpdatePlanner(MetaMgr mm){
        this.mm = mm;
    }

    @Override
    public int executeInsert(InsertData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.getTblname(), mm);
        UpdateScan us = (UpdateScan) p.open();
        us.insert();
        Iterator<Constant> iter = data.getVals().iterator();
        for(String fldname : data.getFlds()){
            Constant val = iter.next();
            us.setConstant(fldname, val);
        }
        us.close();
        return 1;
    }

    @Override
    public int executeDelete(DeleteData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.getTblname(), mm);
        p = new SelectPlan(p, data.getPred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while(us.nextPtr()){
            us.delete();
            count++;
        }
        us.close();
        return count;
    }

    @Override
    public int executeModify(ModifyData data, Transaction tx) {
        Plan p = new TablePlan(tx, data.getTblname(), mm);
        p = new SelectPlan(p, data.getPred());
        UpdateScan us = (UpdateScan) p.open();
        int count = 0;
        while(us.nextPtr()) {
            Constant val = data.getNewval().evaluate(us);
            us.setConstant(data.getFldname(), val);
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
