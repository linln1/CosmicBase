package metadata.mgr;

import excutor.impl.TableScan;
import metadata.MetaIndex;
import metadata.MetaStat;
import mvcc.Transaction;
import record.Layout;
import record.Schema;

import java.util.HashMap;
import java.util.Map;

import static metadata.mgr.TableMgr.MAX_NAME;
/**
 * indexmgr 和 tablemgr功能类似
 */
public class IndexMgr {

    private Layout layout;
    private TableMgr tblmgr;
    private StatMgr statmgr;

    public IndexMgr(boolean isnew, TableMgr tblmgr, StatMgr statmgr, Transaction tx) {
        if (isnew) {
            Schema sch = new Schema();
            sch.addStringField("indexname", MAX_NAME);
            sch.addStringField("tablename", MAX_NAME);
            sch.addStringField("fieldname", MAX_NAME);
            tblmgr.createTable("idxcat", sch, tx);
        }
        this.tblmgr = tblmgr;
        this.statmgr = statmgr;
        layout = tblmgr.getLayout("idxcat", tx);
    }

    /**
     * 创建一个特定域的特定类型的索引
     * 给每个索引分配一个独一无二的ID，它的信息存储在idxcat表中
     * @param idxname
     * @param tblname
     * @param fldname
     * @param tx
     */
    public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
        TableScan ts;
        ts = new TableScan(tx, "idxcat", layout);
        ts.insert();
        ts.setString("indexname", idxname);
        ts.setString("tablename", tblname);
        ts.setString("fieldname", fldname);
        ts.close();
    }

    /**
     * 返回一个map，包含所有的在特定表中的index的信息
     * @param tblname
     * @param tx
     * @return
     */
    public Map<String, MetaIndex> getIndexInfo(String tblname, Transaction tx) {
        Map<String,MetaIndex> result = new HashMap<String,MetaIndex>();
        TableScan ts = new TableScan(tx, "idxcat", layout);
        while (ts.nextPtr()) {
            if (ts.getAsString("tablename").equals(tblname)) {
                String idxname = ts.getAsString("indexname");
                String fldname = ts.getAsString("fieldname");
                Layout tblLayout = tblmgr.getLayout(tblname, tx);
                MetaStat tblsi = statmgr.getStatInfo(tblname, tblLayout, tx);
                MetaIndex ii = new MetaIndex(idxname, fldname, tblLayout.schema(), tx, tblsi);
                result.put(fldname, ii);
            }
        }
        ts.close();
        return result;
    }

}
