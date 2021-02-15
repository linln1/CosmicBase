package plan.Impl;

import excutor.Scan;
import excutor.impl.SelectScan;
import plan.Plan;
import query.Predicate;
import record.Schema;

public class SelectPlan implements Plan{
    private Plan p;
    private Predicate pred;


    public SelectPlan(Plan p, Predicate pred) {
        this.p = p;
        this.pred = pred;
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new SelectScan(s, pred);
    }

    @Override
    public int blockAccessed() {
        return p.blockAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput() / pred.reductionFactor(p);
    }

    @Override
    public int distinctValues(String fldname) {
        if (pred.equatesWithConstant(fldname) != null) {
            return 1;
        } else {
            String fldname2 = pred.equatesWithField(fldname);
            if (fldname2 != null) {
                return Math.min(p.distinctValues(fldname),
                        p.distinctValues(fldname2));
            } else {
                return p.distinctValues(fldname);
            }
        }
    }

    @Override
    public Schema schema() {
        return p.schema();
    }
}
