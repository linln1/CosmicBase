package plan.Impl;

import excutor.Scan;
import excutor.impl.TableScan;
import metadata.MetaStat;
import metadata.mgr.MetaMgr;
import mvcc.Transaction;
import plan.Plan;
import record.Layout;
import record.Schema;

public class TablePlan implements Plan {
    private String tblname;
    private Transaction tx;
    private Layout layout;
    private MetaStat ms;

    public TablePlan(Transaction tx, String tblname, MetaMgr mm) {
        this.tblname = tblname;
        this.tx = tx;
        layout = mm.getLayout(tblname, tx);
        ms = mm.getStatInfo(tblname, layout, tx);
    }

    @Override
    public Scan open() {
        return new TableScan(tx, tblname, layout);
    }

    @Override
    public int blockAccessed() {
        return ms.getNumBlocks();
    }

    @Override
    public int recordsOutput() {
        return ms.getNumRecs();
    }

    @Override
    public int distinctValues(String fldname) {
        return ms.getDistinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return layout.schema();
    }
}
