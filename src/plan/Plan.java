package plan;

import excutor.Scan;
import record.Schema;

/**
 * 每个关系代数算符都有一个plan
 */
public interface Plan {
    public Scan open();

    public int blockAccessed();

    public int recordsOutput();

    public int distinctValues(String fldname);

    public Schema schema();

}
