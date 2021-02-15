package metadata.mgr;

import excutor.impl.TableScan;
import metadata.MetaStat;
import mvcc.Transaction;
import record.Layout;

import java.util.HashMap;
import java.util.Map;

/**
 * 负责维护每个表的统计信息
 * 不将信息存储到数据库中，他在数据库启动的时候计算统计信息，并且定期维护
 */
public class StatMgr {
    private TableMgr tblMgr;
    private Map<String, MetaStat> tablestats;
    private int numcalls;

    /**
     * 遍历整个数据库，获取初始统计信息
     * @param tblMgr
     * @param tx
     */
    public StatMgr(TableMgr tblMgr, Transaction tx) {
        this.tblMgr = tblMgr;
        refreshStatistics(tx);
    }

    /**
     * 返回特定表的统计信息
     * @param tblname
     * @param layout
     * @param tx
     * @return
     */
    public synchronized MetaStat getStatInfo(String tblname,
                                             Layout layout, Transaction tx) {
        numcalls++;
        if (numcalls > 100) {
            refreshStatistics(tx);
        }
        MetaStat si = tablestats.get(tblname);
        if (si == null) {
            si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        return si;
    }

    private synchronized void refreshStatistics(Transaction tx) {
        tablestats = new HashMap<String,MetaStat>();
        numcalls = 0;
        Layout tcatlayout = tblMgr.getLayout("tblcat", tx);
        TableScan tcat = new TableScan(tx, "tblcat", tcatlayout);
        while(tcat.nextPtr()) {
            String tblname = tcat.getAsString("tblname");
            Layout layout = tblMgr.getLayout(tblname, tx);
            MetaStat si = calcTableStats(tblname, layout, tx);
            tablestats.put(tblname, si);
        }
        tcat.close();
    }

    private synchronized MetaStat calcTableStats(String tblname,
                                                 Layout layout, Transaction tx) {
        int numRecs = 0;
        int numblocks = 0;
        TableScan ts = new TableScan(tx, tblname, layout);
        while (ts.nextPtr()) {
            numRecs++;
            numblocks = ts.getRid().getBlkNum() + 1;
        }
        ts.close();
        return new MetaStat(numblocks, numRecs);
    }



}
