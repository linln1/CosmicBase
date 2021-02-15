package plan.Impl;

import mvcc.Transaction;
import parse.*;
import plan.Plan;

/**
 * 用来执行sql语句的对象
 */
public class Planner {
    private QueryPlanner qp;
    private UpdatePlanner up;

    public Planner(QueryPlanner qp, UpdatePlanner up){
        this.qp = qp;
        this.up = up;
    }

    public Plan createQueryPlan(String qry, Transaction tx){
        Parser parser = new Parser(qry);
        QueryData qdata = parser.select();
        verifyQuery(qdata);
        return qp.createPlan(qdata, tx);
    }

    public int executeUpdate(String cmd, Transaction tx) {
        Parser parser = new Parser(cmd);
        Object data = parser.updateCmd();
        verifyUpdate(data);
        if (data instanceof InsertData) {
            return up.executeInsert((InsertData)data, tx);
        } else if (data instanceof DeleteData) {
            return up.executeDelete((DeleteData)data, tx);
        } else if (data instanceof ModifyData) {
            return up.executeModify((ModifyData)data, tx);
        } else if (data instanceof CreateTable) {
            return up.executeCreateTable((CreateTable)data, tx);
        } else if (data instanceof CreateView) {
            return up.executeCreateView((CreateView)data, tx);
        } else if (data instanceof CreateIndex) {
            return up.executeCreateIndex((CreateIndex)data, tx);
        } else {
            return 0;
        }
    }

    private void verifyQuery(QueryData qdata) {
    }

    private void verifyUpdate(Object data) {
    }
}
