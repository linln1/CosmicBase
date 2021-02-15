package query;

import excutor.Scan;
import record.Schema;

/**
 * sql 表达式的接口
 */
public class Expression {
    private Constant val = null;
    private String fldname;

    public Expression(Constant val , String fldname){
        this.val = val;
        this.fldname = fldname;
    }

    public Expression(Constant val){
        this.val = val;
    }

    public Expression(String fldname){
        this.fldname = fldname;
    }

    public Constant evaluate(Scan s){
        return (val != null) ? val : (Constant) s.getVal(fldname);
    }

    public boolean isFieldName(){
        return fldname != null;
    }

    public Constant asConstant() {return val;}

    public String asFieldName() {return fldname;}

    public boolean appliesTo(Schema sch) { return (val != null) ? true : sch.hasField(fldname);}

    @Override
    public String toString() {return (val != null) ? val.toString() : fldname ;}
}
