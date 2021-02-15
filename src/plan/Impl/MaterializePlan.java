package plan.Impl;

import excutor.Scan;
import excutor.UpdateScan;
import mvcc.Transaction;
import plan.Plan;
import record.Layout;
import record.Schema;
import record.TempTable;

/**
 * materialize 运算符的 Plan类
 */
public class MaterializePlan implements Plan{

    private Plan srcplan;
    private Transaction tx;

    public MaterializePlan(Transaction tx, Plan p) {
        this.srcplan = p;
        this.tx = tx;
    }

    @Override
    public Scan open(){
        Schema sch = srcplan.schema();
        TempTable temp = new TempTable(tx, sch);
        Scan src = srcplan.open();
        UpdateScan dest = temp.open();
        while (src.nextPtr()) {
            dest.insert();
            for (String fldname : sch.fields()) {
                dest.setConstant(fldname, src.getVal(fldname));
            }
        }
        src.close();
        dest.StartPtr();
        return dest;
    }

    @Override
    public int blockAccessed() {
        Layout layout = new Layout(srcplan.schema());
        double rpb = (double) (tx.blockSize() / layout.slotSize());
        return (int) Math.ceil(srcplan.recordsOutput() / rpb);
    }

    @Override
    public int recordsOutput() {
        return srcplan.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return srcplan.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return srcplan.schema();
    }
}
