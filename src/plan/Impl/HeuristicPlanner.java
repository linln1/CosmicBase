package plan.Impl;

import metadata.mgr.MetaMgr;
import mvcc.Transaction;
import parse.QueryData;
import plan.Plan;
import plan.QueryPlanner;

import java.util.ArrayList;
import java.util.Collection;

/**
 * 经过优化的启发式query
 */
public class HeuristicPlanner implements QueryPlanner {
    private Collection<TablePlanner> tablePlanners = new ArrayList<>();
    private MetaMgr mm;

    public HeuristicPlanner(MetaMgr mm){
        this.mm = mm;
    }

    /**
     * 创建一个优化的左深查询plan，使用下面的启发式方法
     * H1.选择最小的table放在join运算的最左边
     * H2.继续连接其余的table，使得每次都产生最小的输出
     * @param data
     * @param tx
     * @return
     */
    @Override
    public Plan createPlan(QueryData data, Transaction tx) {
        // Step 1:  Create a TablePlanner object for each mentioned table
        for (String tblname : data.tables()) {
            TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mm);
            tablePlanners.add(tp);
        }

        // Step 2:  Choose the lowest-size simpledb.plan to begin the join order
        Plan currentplan = getLowestSelectPlan();

        // Step 3:  Repeatedly add a simpledb.plan to the join order
        while (!tablePlanners.isEmpty()) {
            Plan p = getLowestJoinPlan(currentplan);
            if (p != null) {
                currentplan = p;
            } else  // no applicable join
            {
                currentplan = getLowestProductPlan(currentplan);
            }
        }

        // Step 4.  Project on the field names and return
        return new ProjectPlan(currentplan, data.fields());
    }

    private Plan getLowestSelectPlan(){
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tablePlanners){
            Plan plan = tp.makeSelectPlan();
            if(bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()){
                besttp = tp;
                bestplan = plan;
            }
        }
        tablePlanners.remove(besttp);
        return bestplan;
    }

    private Plan getLowestJoinPlan(Plan p){
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeJoinPlan(p);
            if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
                besttp = tp;
                bestplan = plan;
            }
        }
        if (bestplan != null)
            tablePlanners.remove(besttp);
        return bestplan;
    }

    private Plan getLowestProductPlan(Plan p){
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tablePlanners) {
            Plan plan = tp.makeProductPlan(p);
            if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
                besttp = tp;
                bestplan = plan;
            }
        }
        tablePlanners.remove(besttp);
        return bestplan;
    }

    public void setTablePlanners(Planner p){

    }
}
