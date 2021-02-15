package plan;

import mvcc.Transaction;
import parse.QueryData;

public interface QueryPlanner {
    public Plan createPlan(QueryData data, Transaction tx);
}
