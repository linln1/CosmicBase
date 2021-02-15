package parse;

import record.Schema;

public class CreateTable {
    private String tblname;
    private Schema sch;

    public CreateTable(String tblname, Schema sch) {
        this.tblname = tblname;
        this.sch = sch;
    }

    public String getTblname() {
        return tblname;
    }

    public Schema getSch() {
        return sch;
    }
}
