package plan.Impl;

import metadata.mgr.MetaMgr;
import mvcc.Transaction;
import parse.Parser;
import parse.QueryData;
import plan.Plan;

import java.util.ArrayList;
import java.util.List;

/**
 * An improved query planner
 */
public class QueryPlanner implements plan.QueryPlanner {
    private MetaMgr mm;

    public QueryPlanner(MetaMgr mm) {
        this.mm = mm;
    }

    /**
     * 首先将所有的表和视图笛卡尔积
     * 然后利用谓词进行选择，最终投影到field list上
     * @param data
     * @param tx
     * @return
     */
    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        //Step 1: Create a simpledb.plan for each mentioned table or view.
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            String viewdef;
            viewdef = mm.getViewDef(tblname, tx);
            if (viewdef != null) { // Recursively simpledb.plan the view.
                Parser parser = new Parser(viewdef);
                QueryData viewdata = parser.select();
                plans.add(createPlan(viewdata, tx));
            }
            else {
                plans.add(new TablePlan(tx, tblname, mm));
            }
        }

        //Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextplan : plans) {
            //Try both orderings and choose the one having lowest cost
            Plan p1 = new ProductPlan(nextplan, p);
            Plan p2 = new ProductPlan(p, nextplan);
            if(p1.blockAccessed() < p2.blockAccessed()){
                p = p1;
            }else{
                p = p2;
            }
        }

        //Step 3: Add a selection simpledb.plan for the predicate
        p = new SelectPlan(p, data.pred());

        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }
}
