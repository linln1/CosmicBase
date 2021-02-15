package plan.Impl;

import excutor.impl.ProductScan;
import excutor.Scan;
import plan.Plan;
import record.Schema;

public class ProductPlan implements Plan{
    private Plan p1, p2;
    private Schema schema = new Schema();

    public ProductPlan(Plan p1, Plan p2) {
        this.p1 = p1;
        this.p2 = p2;
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
    }

    @Override
    public Scan open() {
        Scan s1 = p1.open();
        Scan s2 = p2.open();
        return (Scan) new ProductScan(s1, s2);
    }

    @Override
    public int blockAccessed() {
        return p1.blockAccessed() + (p1.blockAccessed() * p2.blockAccessed());
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() * p2.recordsOutput();
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
        return schema;
    }

}
