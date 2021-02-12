package parse;

import excutor.Scanner;

public class Expression {// 表达式 = {常量， 字符串}
    private Constant cons = null;//常量
    private String field = null;//字符串

    public Expression(Constant val){
        this.cons = val;
    }

    public Expression(String field){
        this.field = field;
    }
    /**
     * Evaluate the expression with respect to the
     * current record of the specified scan.
     * @param: Scan s
     * @return: the value of the expression, as a Constant
     */
    public Constant evaluate(Scanner s){
        return (cons != null) ? cons : s.getVal(field);
    }

    //public boolean

}
