package parse;

import query.Constant;

import java.util.List;

public class InsertData {

    private String tblname;
    private List<String> flds;
    private List<Constant> vals;

    public InsertData(String tblname, List<String> flds, List<Constant> vals){
        this.tblname = tblname;
        this.flds = flds;
        this.vals = vals;
    }

    public String getTblname(){
        return tblname;
    }

    public List<String> getFlds(){return flds;}

    public List<Constant> getVals(){
        return vals;
    }

}
