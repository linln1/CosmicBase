package parse;

import query.Predicate;

public class DeleteData {
    private String tblname;
    private Predicate pred;

    public DeleteData(String tblname, Predicate pred){
        this.tblname = tblname;
        this.pred = pred;
    }

    public String getTblname(){
        return tblname;
    }

    public Predicate getPred(){
        return pred;
    }
}
