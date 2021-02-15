package plan.Impl;

import metadata.MetaIndex;
import metadata.mgr.MetaMgr;
import mvcc.Transaction;
import plan.Plan;
import query.Constant;
import query.Predicate;
import record.Schema;

import java.util.Map;

/**
 * 这个类包含了planning一个table的方法
 */
public class TablePlanner {
    private TablePlan myplan;
    private Predicate mypred;
    private Schema myschema;
    private Map<String, MetaIndex> indexes;
    private Transaction tx;

    public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetaMgr mm) {
        this.mypred  = mypred;
        this.tx  = tx;
        myplan   = new TablePlan(tx, tblname, mm);
        myschema = myplan.schema();
        indexes  = mm.getIndexInfo(tblname, tx);
    }

    /**
     * 对table构造一个select plan,这个plan将使用一个indexselect
     * @return
     */
    public Plan makeSelectPlan(){
        Plan p = makeSelectPlan();
        if(p == null){
            p = myplan;
        }
        return addSelectPred(p);
    }

    public Plan makeJoinPlan(Plan currentPlan){
        Schema cursch = currentPlan.schema();
        Predicate joinpred = mypred.joinSubPred(myschema, cursch);
        if(joinpred == null){
            return null;
        }
        Plan p = makeIndexJoin(currentPlan, cursch);
        if(p == null){
            p = makeProductJoin(currentPlan, cursch);
        }
        return p;
    }

    public Plan makeProductPlan(Plan current) {
        Plan p = addSelectPred(myplan);
        return new MultiBufferProductPlan(tx, current, p);
    }

    private Plan makeIndexSelect() {
        for (String fldname : indexes.keySet()) {
            Constant val = mypred.equatesWithConstant(fldname);
            if (val != null) {
                MetaIndex ii = indexes.get(fldname);
                System.out.println("simpledb.index on " + fldname + " used");
                return new IndexSelectPlan(myplan, ii, val);
            }
        }
        return null;
    }

    private Plan makeIndexJoin(Plan current, Schema currsch) {
        for (String fldname : indexes.keySet()) {
            String outerfield = mypred.equatesWithField(fldname);
            if (outerfield != null && currsch.hasField(outerfield)) {
                MetaIndex ii = indexes.get(fldname);
                Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan makeProductJoin(Plan current, Schema currsch) {
        Plan p = makeProductPlan(current);
        return addJoinPred(p, currsch);
    }

    private Plan addSelectPred(Plan p) {
        Predicate selectpred = mypred.selectSubPred(myschema);
        if (selectpred != null) {
            return new SelectPlan(p, selectpred);
        } else {
            return p;
        }
    }

    private Plan addJoinPred(Plan p, Schema currsch) {
        Predicate joinpred = mypred.joinSubPred(currsch, myschema);
        if (joinpred != null) {
            return new SelectPlan(p, joinpred);
        } else {
            return p;
        }
    }
}
