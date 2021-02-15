package plan.Impl;

import excutor.Scan;
import excutor.impl.MergeJoinScan;
import excutor.impl.SortScan;
import mvcc.Transaction;
import plan.Plan;
import record.Schema;

import java.util.Arrays;
import java.util.List;

public class MergeJoinPlan implements Plan {
    private Plan p1, p2;
    private String fld1, fld2;
    private Schema sch = new Schema();

    public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fld1, String fld2){
        this.fld1 = fld1;
        List<String> sortlist1 = Arrays.asList(fld1);
        this.p1 = new SortPlan(tx, p1, sortlist1);

        this.fld2 = fld2;
        List<String> sortlist2 = Arrays.asList(fld2);
        this.p2 = new SortPlan(tx, p2, sortlist2);

        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        SortScan s2 = (SortScan) p2.open();
        return new MergeJoinScan(s1, s2, fld1, fld2);
    }

    @Override
    public int blockAccessed() {
        return p1.blockAccessed() + p2.blockAccessed();
    }

    @Override
    public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fld1),
                p2.distinctValues(fld2));
        return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
    }

    @Override
    public int distinctValues(String fldname) {
        if (p1.schema().hasField(fldname)) {
            return p1.distinctValues(fldname);
        } else {
            return p2.distinctValues(fldname);
        }
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
