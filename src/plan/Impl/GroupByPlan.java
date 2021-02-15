package plan.Impl;

import excutor.Scan;
import excutor.impl.GroupByScan;
import mvcc.Transaction;
import plan.AggregationFn;
import plan.Plan;
import record.Schema;

import java.util.List;

/**
 * groupby 的Excutor
 */
public class GroupByPlan implements Plan{

    private Plan p;
    private List<String> groupfields;
    private List<AggregationFn> aggfns;
    private Schema sch = new Schema();

    public GroupByPlan(Transaction tx, Plan p, List<String> groupfields, List<AggregationFn> aggfns){
        this.p = new SortPlan(tx, p, groupfields);
        this.groupfields = groupfields;
        this.aggfns = aggfns;
        for (String fldname : groupfields){
            sch.add(fldname, p.schema());
        }
        for (AggregationFn fn : aggfns){
            sch.addIntField(fn.fieldName());
        }
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new GroupByScan(s, groupfields, aggfns);
    }

    @Override
    public int blockAccessed() {
        return 0;
    }

    @Override
    public int recordsOutput() {
        return 0;
    }

    @Override
    public int distinctValues(String fldname) {
        return 0;
    }

    @Override
    public Schema schema() {
        return null;
    }
}
