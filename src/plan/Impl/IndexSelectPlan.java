package plan.Impl;

import excutor.Scan;
import excutor.impl.IndexSelectScan;
import excutor.impl.TableScan;
import index.Index;
import metadata.MetaIndex;
import plan.Plan;
import query.Constant;
import record.Schema;

public class IndexSelectPlan implements Plan {
    private Plan p;
    private MetaIndex mi;
    private Constant val;

    public IndexSelectPlan(Plan p, MetaIndex mi, Constant val){
        this.p = p;
        this.mi = mi;
        this.val = val;
    }

    @Override
    public Scan open() {
        TableScan ts = (TableScan) p.open();
        Index idx = mi.open();
        return new IndexSelectScan(ts, idx, val);
    }

    /**
     * 估计用来计算index select所需要获得的block的个数
     * 和遍历开销+匹配数据的records 一样
     * @return
     */
    @Override
    public int blockAccessed() {
        return mi.getBlockNum() + recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return mi.getNumRecs();
    }

    @Override
    public int distinctValues(String fldname) {
        return mi.getDistinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return p.schema();
    }
}
