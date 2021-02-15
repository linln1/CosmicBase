package index.hash;

import excutor.impl.TableScan;
import index.Index;
import mvcc.Transaction;
import query.Constant;
import record.Layout;
import record.RId;

/**
 * 分配固定的bucket(100)
 * 每一个bucket都是由record组成的file
 */
public class HashIndex implements Index {
    public static int NUM_BUCKETS = 100;
    private Transaction tx;
    private String idxname;
    private Layout layout;
    private Constant searchkey = null;
    private TableScan ts = null;

    public HashIndex(Transaction tx, String idxname, Layout layout) {
        this.tx = tx;
        this.idxname = idxname;
        this.layout = layout;
    }


    @Override
    public void startPtr(Constant searchkey) {
        close();
        this.searchkey = searchkey;
        int bucket = searchkey.hashCode() % NUM_BUCKETS;
        String tblname = idxname + bucket;
        ts = new TableScan(tx, tblname, layout);
    }

    @Override
    public boolean nextPtr() {
        while (ts.nextPtr() ) {
            if (ts.getVal("dataval").equals(searchkey)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public RId getDataRId() {
        int blknum = ts.getAsInt("block");
        int id = ts.getAsInt("id");
        return new RId(blknum, id);
    }

    @Override
    public void insert(Constant dataval, RId datarid) {
        startPtr(dataval);
        ts.insert();
        ts.setInt("block", datarid.getBlkNum());
        ts.setInt("id", datarid.getSlot());
        ts.setConstant("dataval", dataval);
    }

    @Override
    public void delete(Constant dataval, RId datarid) {
        startPtr(dataval);
        while(nextPtr()) {
            if (getDataRId().equals(datarid)) {
                ts.delete();
                return;
            }
        }
    }

    @Override
    public void close() {
        if (ts != null) {
            ts.close();
        }
    }

    /**
     * 返回搜索Index文件的成本
     * 这个方法假设所有的bucket都是同样大小的,所以花费就是桶的大小
     * @param numblocks
     * @param rpb
     * @return
     */
    public static int searchCost(int numblocks, int rpb){
        return numblocks / HashIndex.NUM_BUCKETS;
    }
}
