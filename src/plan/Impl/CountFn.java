package plan.Impl;

import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import excutor.Scan;
import plan.AggregationFn;
import query.Constant;

public class CountFn implements AggregationFn {
    private String fldname;
    private int count;

    public CountFn(String fldname) {this.fldname = fldname ;}

    @Override
    public void processFirst(Scan s) {count = 1;}

    @Override
    public void processNext(Scan s) {count++;}

    @Override
    public String fieldName() {return "countof" + fldname ;}

    @Override
    public Constant value() {return new Constant(count);}
}
