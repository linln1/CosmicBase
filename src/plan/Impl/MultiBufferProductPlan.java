package plan.Impl;

import excutor.Scan;
import excutor.UpdateScan;
import excutor.impl.MultiBufferProductScan;
import mvcc.Transaction;
import plan.Plan;
import record.Schema;
import record.TempTable;

/**
 * 多缓冲区版本的product
 */
public class MultiBufferProductPlan implements Plan {
    private Transaction tx;
    private Plan lp, rp;
    private Schema sch = new Schema();

    public MultiBufferProductPlan(Transaction tx, Plan lp, Plan rp) {
        this.tx = tx;
        this.lp = new MaterializePlan(tx, lp);
        this.rp = rp;
        sch.addAll(lp.schema());
        sch.addAll(rp.schema());
    }

    @Override
    public Scan open() {
        Scan leftscan = lp.open();
        TempTable tt = copyRecordsFrom(rp);
        return new MultiBufferProductScan(tx, leftscan, tt.getTableName(), tt.getLayout());
    }

    private TempTable copyRecordsFrom(Plan rp) {
        Scan   src = rp.open();
        Schema sch = rp.schema();
        TempTable t = new TempTable(tx, sch);
        UpdateScan dest = (UpdateScan) t.open();
        while (src.nextPtr()) {
            dest.insert();
            for (String fldname : sch.fields()) {
                dest.setConstant(fldname, src.getVal(fldname));
            }
        }
        src.close();
        dest.close();
        return t;
    }

    /**
     * 返回一个执行query所需的块的个数。
     * 公式是 Block(prod(p1,p2)) = Block(p2) + Block(p1) * C(p2)
     * C(p2)是p2的chunks的个数
     * 方法使用当前的空闲buffers个数来计算C(p2),所以这个值可能在scan open的时候变化
     * @return
     */
    @Override
    public int blockAccessed() {
        int avail = tx.availableBuffers();
        int size = new MaterializePlan(tx, rp).blockAccessed();
        int numchunks = size / avail;
        return rp.blockAccessed() +
                (rp.blockAccessed() * numchunks);
    }

    /**
     * 估计输出的records的个数
     * 公式为 Rec(prod(p1,p2)) = Rec(p1) * Rec(p2)
     * @return
     */
    @Override
    public int recordsOutput() {
        return lp.recordsOutput() * rp.recordsOutput();
    }

    /**
     * 估计prod中不同的域的值
     * @param fldname
     * @return
     */
    @Override
    public int distinctValues(String fldname) {
        if(lp.schema().hasField(fldname)){
            return lp.distinctValues(fldname);
        }
        else{
            return rp.distinctValues(fldname);
        }
    }

    @Override
    public Schema schema() {
        return sch;
    }


}
