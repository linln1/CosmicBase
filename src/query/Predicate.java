package query;

import excutor.Scan;
import plan.Plan;
import record.Schema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * term 的布尔组合
 */
public class Predicate {

    private List<Term> termList = new ArrayList<>();

    public Predicate(Term term) {
        termList.add(term);
    }

    public Predicate() {

    }

    public void conjoinWith(Predicate predicate) {
        termList.addAll(predicate.termList);
    }

    public boolean holds(Scan s){
        for (Term t : termList){
            if(!t.holds(s)){
                return false;
            }
        }
        return true;
    }

    public int reductionFactor(Plan p){
        int factor = 1;
        for(Term t : termList){
            factor *= t.reductionFactor(p);
        }
        return factor;
    }

    /**
     * 返回子谓词
     * @param sch
     * @return
     */
    public Predicate selectSubPred(Schema sch){
        Predicate result = new Predicate();
        for (Term t : termList){
            if(t.appliesTo(sch)){
                result.termList.add(t);
            }
        }
        if (result.termList.size() == 0){
            return null;
        }
        else{
            return result;
        }
    }

    /**
     * 返回由terms组成的两个特定schema的并,但不是针对单独的某一个schema
     */
    public Predicate joinSubPred(Schema sch1, Schema sch2){
        Predicate result = new Predicate();
        Schema newsch = new Schema();
        newsch.addAll(sch1);
        newsch.addAll(sch2);
        for (Term t : termList) {
            if (!t.appliesTo(sch1)  &&
                    !t.appliesTo(sch2) &&
                    t.appliesTo(newsch)) {
                result.termList.add(t);
            }
        }
        if (result.termList.size() == 0) {
            return null;
        } else {
            return result;
        }
    }


    public Constant equatesWithConstant(String field){
        for(Term t : termList){
            Constant c = t.equatesWithConstant(field);
            if(c != null){
                return c;
            }
        }
        return null;
    }

    /**
     * 决定是否有一个term的形式是"F1=F2",F1是特定域，并且F2是另一个域。
     * 如果由，那么方法返回域的名字
     * 否则返回空
     * @param field
     * @return
     */
    public String equatesWithField(String field){
        for (Term t : termList){
            String s = t.equatesWithField(field);
            if(s!=null){
                return s;
            }
        }
        return null;
    }

    @Override
    public String toString(){
        Iterator<Term> iter = termList.iterator();
        if(!iter.hasNext()){
            return "";
        }
        String result = iter.next().toString();
        while (iter.hasNext()){
            result += " and " + iter.next().toString();
        }
        return result;
    }
}
