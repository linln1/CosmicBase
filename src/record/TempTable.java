package record;

import excutor.UpdateScan;
import excutor.impl.TableScan;
import mvcc.Transaction;

public class TempTable {
    private static int nextTableNum = 0;
    private Transaction tx;
    private String tlbname;
    private Layout layout;

    public TempTable(Transaction tx, Schema sch){
        this.tx = tx;
        tlbname = nextTableName();
        layout = new Layout(sch);
    }

    public UpdateScan open() {
        return new TableScan(tx, tlbname, layout);
    }

    public String getTableName() {
        return tlbname;
    }

    public Layout getLayout() {
        return layout;
    }

    private static synchronized String nextTableName() {
        nextTableNum++;
        return "temp" + nextTableNum;
    }
}
