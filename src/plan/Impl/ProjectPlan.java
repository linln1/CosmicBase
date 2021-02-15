package plan.Impl;

import excutor.Scan;
import excutor.impl.ProjectScan;
import plan.Plan;
import record.Schema;

import java.util.List;

public class ProjectPlan implements Plan {
    private Plan p;
    private Schema schema = new Schema();

    public ProjectPlan(Plan p, List<String> fieldList){
        this.p = p;
        for(String fldname : fieldList){
            schema.add(fldname, p.schema());
        }
    }

    @Override
    public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, schema.fields());
    }

    @Override
    public int blockAccessed() {
        return p.blockAccessed();
    }

    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
