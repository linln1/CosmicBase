package metadata.mgr;

import excutor.impl.TableScan;
import mvcc.Transaction;
import record.Layout;
import record.Schema;

import java.util.HashMap;
import java.util.Map;

public class TableMgr {
    public static final int MAX_NAME = 16;
    private Layout tcatLayout, fcatLayout;

    public TableMgr(boolean isNew, Transaction tx) {
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField("tblname", MAX_NAME);
        tcatSchema.addIntField("slotsize");
        tcatLayout = new Layout(tcatSchema);

        Schema fcatSchema = new Schema();
        fcatSchema.addStringField("tblname", MAX_NAME);
        fcatSchema.addStringField("fldname", MAX_NAME);
        fcatSchema.addIntField("type");
        fcatSchema.addIntField("length");
        fcatSchema.addIntField("offset");
        fcatLayout = new Layout(fcatSchema);

        if (isNew) {
            createTable("tblcat", tcatSchema, tx);
            createTable("fldcat", fcatSchema, tx);
        }
    }

    public void createTable(String tblname, Schema sch, Transaction tx) {
        Layout layout = new Layout(sch);
        // insert one simpledb.record into tblcat
        TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
        tcat.insert();
        tcat.setString("tblname", tblname);
        tcat.setInt("slotsize", layout.slotSize());
        tcat.close();

        // insert a simpledb.record into fldcat for each field
        TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
        for (String fldname : sch.fields()) {
            fcat.insert();
            fcat.setString("tblname", tblname);
            fcat.setString("fldname", fldname);
            fcat.setInt   ("type",   sch.type(fldname));
            fcat.setInt   ("length", sch.length(fldname));
            fcat.setInt   ("offset", layout.offset(fldname));
        }
        fcat.close();
    }

    public Layout getLayout(String tblname, Transaction tx) {
        int size = -1;
        TableScan tcat = new TableScan(tx, "tblcat", tcatLayout);
        while(tcat.nextPtr()) {
            if(tcat.getAsString("tblname").equals(tblname)) {
                size = tcat.getAsInt("slotsize");
                break;
            }
        }
        tcat.close();

        Schema sch = new Schema();
        Map<String,Integer> offsets = new HashMap<String,Integer>();
        TableScan fcat = new TableScan(tx, "fldcat", fcatLayout);
        while(fcat.nextPtr()) {
            if(fcat.getAsString("tblname").equals(tblname)) {
                String fldname = fcat.getAsString("fldname");
                int fldtype    = fcat.getAsInt("type");
                int fldlen     = fcat.getAsInt("length");
                int offset     = fcat.getAsInt("offset");
                offsets.put(fldname, offset);
                sch.addField(fldname, fldtype, fldlen);
            }
        }
        fcat.close();
        return new Layout(sch, offsets, size);
    }


}
