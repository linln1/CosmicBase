package parse;

import com.sun.org.apache.xpath.internal.operations.Mod;
import query.Expression;
import query.Predicate;

public class ModifyData {
    private String tblname;
    private String fldname;
    private Expression newval;
    private Predicate pred;

    public ModifyData(String tblname, String fldname, Expression newval, Predicate pred) {
        this.tblname = tblname;
        this.fldname = fldname;
        this.newval = newval;
        this.pred = pred;
    }

    public String getTblname() {
        return tblname;
    }

    public String getFldname() {
        return fldname;
    }

    public Expression getNewval() {
        return newval;
    }

    public Predicate getPred() {
        return pred;
    }
}
