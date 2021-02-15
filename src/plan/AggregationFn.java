package plan;

import excutor.Scan;
import query.Constant;

/**
 * aggregation功能代数
 */
public interface AggregationFn {
    void processFirst(Scan s);
    void processNext(Scan s);

    public String fieldName();
    public Constant value();
}
