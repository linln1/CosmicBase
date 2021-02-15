package metadata.mgr;

import metadata.MetaIndex;
import metadata.MetaStat;
import mvcc.Transaction;
import record.Layout;
import record.Schema;

import java.util.Map;

public class MetaMgr {
    private static TableMgr  tblmgr;
    private static ViewMgr   viewmgr;
    private static StatMgr   statmgr;
    private static IndexMgr  idxmgr;

    public MetaMgr(boolean isnew, Transaction tx) {
        tblmgr  = new TableMgr(isnew, tx);
        viewmgr = new ViewMgr(isnew, tblmgr, tx);
        statmgr = new StatMgr(tblmgr, tx);
        idxmgr  = new IndexMgr(isnew, tblmgr, statmgr, tx);
    }

    public void createTable(String tblname, Schema sch, Transaction tx) {
        tblmgr.createTable(tblname, sch, tx);
    }

    public Layout getLayout(String tblname, Transaction tx) {
        return tblmgr.getLayout(tblname, tx);
    }

    public void createView(String viewname, String viewdef, Transaction tx) {
        viewmgr.createView(viewname, viewdef, tx);
    }

    public String getViewDef(String viewname, Transaction tx) {
        return viewmgr.getViewDef(viewname, tx);
    }

    public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        idxmgr.createIndex(idxname, tblname, fldname, tx);
    }

    public Map<String, MetaIndex> getIndexInfo(String tblname, Transaction tx) {
        return idxmgr.getIndexInfo(tblname, tx);
    }

    public MetaStat getStatInfo(String tblname, Layout layout, Transaction tx) {
        return statmgr.getStatInfo(tblname, layout, tx);
    }
}
