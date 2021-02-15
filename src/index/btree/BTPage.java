package index.btree;

import file.Block;
import mvcc.Transaction;
import query.Constant;
import record.Layout;
import record.RId;
import record.Schema;

import static java.sql.Types.INTEGER;

/**
 * B-tree 目录和叶子页面有很多共性
 * 特别的讲，他们的记录是按顺序存储的，还会根据情况分页
 * 一个 BTNode 对象包含这些普通功能
 */
public class BTPage {

    private Transaction tx;
    private Block currentblk;
    private Layout layout;

    /**
     *
     */
    public BTPage(Transaction tx, Block currentblk, Layout layout){
        this.tx = tx;
        this.currentblk = currentblk;
        this.layout = layout;
        tx.pin(currentblk);
    }

    /**
     * 计算第一条有searchkey的record的位置，然后返回它之前的位子
     */
    public int findSlotBefore(Constant searchKey){
        int slot = 0;
        while (slot < getNumRecs() && getDataVal(slot).compareTo(searchKey) < 0){
            slot++;
        }
        return slot - 1;
    }

    public void close(){
        if(currentblk != null){
            tx.unpin(currentblk);
        }
        currentblk = null;
    }

    public boolean isFull(){
        return slotpos(getNumRecs() + 1) >= tx.blockSize();
    }

    public Block split(int splitpos, int flag){
        Block newblk = appendNew(flag);
        BTPage newpage = new BTPage(tx, newblk, layout);
        transferRecs(splitpos, newpage);
        newpage.setFlag(flag);
        newpage.close();
        return newblk;
    }

    public Constant getDataVal(int slot) {
        return getVal(slot, "dataval");
    }

    public int getFlag(){
        return tx.getInt(currentblk, 0);
    }

    public synchronized void setFlag(int val){
        tx.setInt(currentblk, 0, val, true);
    }

    public Block appendNew(int flag) {
        Block blk = tx.append(currentblk.getFilename());
        tx.pin(blk);
        format(blk, flag);
        return blk;
    }

    public void format(Block blk, int flag) {
        tx.setInt(blk, 0, flag, false);
        tx.setInt(blk, Integer.BYTES, 0, false);  // #records = 0
        int recsize = layout.slotSize();
        for (int pos=2*Integer.BYTES; pos+recsize<=tx.blockSize(); pos += recsize) {
            makeDefaultRecord(blk, pos);
        }
    }

    private void makeDefaultRecord(Block blk, int pos) {
        for (String fldname : layout.schema().fields()) {
            int offset = layout.offset(fldname);
            if (layout.schema().type(fldname) == INTEGER) {
                tx.setInt(blk, pos + offset, 0, false);
            } else {
                tx.setString(blk, pos + offset, "", false);
            }
        }
    }

    public int getChildNum(int slot){
        return getInt(slot, "block");
    }

    public void insertDir(int slot, Constant val, int blknum){
        insert(slot);
        setVal(slot, "dataval", val);
        setInt(slot, "block", blknum);
    }

    public RId getDataRId(int slot){
        return new RId(getInt(slot, "block"), getInt(slot, "id"));
    }

    public void insertLeaf(int slot, Constant val, RId rid) {
        insert(slot);
        setVal(slot, "dataval", val);
        setInt(slot, "block", rid.getBlkNum());
        setInt(slot, "id", rid.getSlot());
    }

    public void delete(int slot) {
        for (int i=slot+1; i<getNumRecs(); i++) {
            copyRecord(i, i-1);
        }
        setNumRecs(getNumRecs()-1);
        return;
    }

    public int getNumRecs() {
        return tx.getInt(currentblk, Integer.BYTES);
    }

    private int getInt(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getInt(currentblk, pos);
    }

    private String getString(int slot, String fldname) {
        int pos = fldpos(slot, fldname);
        return tx.getString(currentblk, pos);
    }

    private Constant getVal(int slot, String fldname) {
        int type = layout.schema().type(fldname);
        if (type == INTEGER) {
            return new Constant(getInt(slot, fldname));
        } else {
            return new Constant(getString(slot, fldname));
        }
    }

    private void setInt(int slot, String fldname, int val) {
        int pos = fldpos(slot, fldname);
        tx.setInt(currentblk, pos, val, true);
    }

    private void setString(int slot, String fldname, String val) {
        int pos = fldpos(slot, fldname);
        tx.setString(currentblk, pos, val, true);
    }

    private void setVal(int slot, String fldname, Constant val) {
        int type = layout.schema().type(fldname);
        if (type == INTEGER) {
            setInt(slot, fldname, val.asInt());
        } else {
            setString(slot, fldname, val.asString());
        }
    }

    private void setNumRecs(int n) {
        tx.setInt(currentblk, Integer.BYTES, n, true);
    }

    private void insert(int slot) {
        for (int i=getNumRecs(); i>slot; i--) {
            copyRecord(i-1, i);
        }
        setNumRecs(getNumRecs()+1);
    }

    private void copyRecord(int from, int to) {
        Schema sch = layout.schema();
        for (String fldname : sch.fields()) {
            setVal(to, fldname, getVal(from, fldname));
        }
    }

    private void transferRecs(int slot, BTPage dest) {
        int destslot = 0;
        while (slot < getNumRecs()) {
            dest.insert(destslot);
            Schema sch = layout.schema();
            for (String fldname : sch.fields()) {
                dest.setVal(destslot, fldname, getVal(slot, fldname));
            }
            delete(slot);
            destslot++;
        }
    }

    private int fldpos(int slot, String fldname) {
        int offset = layout.offset(fldname);
        return slotpos(slot) + offset;
    }

    private int slotpos(int slot) {
        int slotsize = layout.slotSize();
        return Integer.BYTES + Integer.BYTES + (slot * slotsize);
    }
}
