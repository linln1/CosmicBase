package plan.Impl;

import excutor.Scan;
import excutor.impl.IndexJoinScan;
import excutor.impl.TableScan;
import index.Index;
import metadata.MetaIndex;
import plan.Plan;
import record.Schema;

/**
 * 与index连接关系代数运算符相关的Plan类
 */
public class IndexJoinPlan implements Plan {
    private Plan p1, p2;
    private MetaIndex mi;
    private String joinField;
    private Schema sch = new Schema();

    public IndexJoinPlan(Plan p1, Plan p2, MetaIndex mi, String joinField){
        this.p1 = p1;
        this.p2 = p2;
        this.mi = mi;
        this.joinField = joinField;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s = p1.open();
        TableScan ts = (TableScan) p2.open();
        Index idx = mi.open();
        return new IndexJoinScan(s, idx, joinField, ts);
    }


    /**
     * 估计能够获取到的用来join的块的个数
     * Block(join(p1,p2,idx)) = Block(p1) + Rec(p1) * Block(idx) + Rec(join(p1,p2,idx))
     * @return
     */
    @Override
    public int blockAccessed() {
        return p1.blockAccessed() + (p1.blockAccessed() * mi.getBlockNum()) +
                recordsOutput();
    }

    @Override
    public int recordsOutput() {
        return p1.blockAccessed() * mi.getBlockNum();
    }

    @Override
    public int distinctValues(String fldname) {
        if(p1.schema().hasField(fldname)){
            return p1.distinctValues(fldname);
        }else{
            return p2.distinctValues(fldname);
        }
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
