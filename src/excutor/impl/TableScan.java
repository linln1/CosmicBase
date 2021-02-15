package excutor.impl;

import com.sun.prism.impl.Disposer;
import excutor.UpdateScan;
import file.Block;
import mvcc.Transaction;
import query.Constant;
import record.Layout;
import record.PageId;
import record.RId;
import record.RecordPage;

import static java.sql.Types.INTEGER;

/**
 * 提供任意大小的record序列的抽象
 */
public class TableScan implements UpdateScan {
    private Transaction tx;
    private Layout layout;
    private RecordPage rp;
    private String filename;
    private int currentslot;

    public TableScan(Transaction tx, String tlbname, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        filename = tlbname + ".tbl";
        if (tx.size(filename) == 0){
            moveToNewBlock();
        }
        else{
            moteToBlock(0);
        }
    }

    private void moteToBlock(int blknum) {
        close();
        Block blk = new Block(filename, blknum);
        rp = new RecordPage(tx, blk, layout);
        currentslot = -1;
    }

    private void moveToNewBlock() {
        close();
        Block blk = tx.append(filename);
        rp = new RecordPage(tx, blk, layout);
        rp.format();
        currentslot = -1;
    }

    @Override
    public void setInt(String field, int val) {
        rp.setInt(currentslot, field, val);
    }


    @Override
    public void setString(String field, String val) {
        rp.setString(currentslot, field, val);
    }

    @Override
    public void setConstant(String field, Constant val) {
        if(layout.schema().type(field) == INTEGER){
            setInt(field, val.asInt());
        }else{
            setString(field, val.asString());
        }
    }


    @Override
    public void insert() {
        currentslot = rp.insertAfter(currentslot);
        while (currentslot < 0) {
            if (atLastBlock()) {
                moveToNewBlock();
            } else {
                moveToBlock(rp.block().getBlknum()+1);
            }
            currentslot = rp.insertAfter(currentslot);
        }
    }

    private void moveToBlock(int blknum) {
        close();
        Block blk = new Block(filename, blknum);
        rp = new RecordPage(tx, blk, layout);
        currentslot = -1;
    }

    private boolean atLastBlock() {
        return rp.block().getBlknum() == tx.size(filename) - 1;
    }


    @Override
    public void delete() {
        rp.delete(currentslot);
    }

    @Override
    public RId getRid() {
        return new RId(rp.block().getBlknum(), currentslot);
    }

    @Override
    public void moveToRid(RId rid) {

    }

    @Override
    public void StartPtr() {
        moveToBlock(0);
    }

    @Override
    public boolean nextPtr() {
        currentslot = rp.nextAfter(currentslot);
        while(currentslot < 0){
            if(atLastBlock()){
                return false;
            }
            moveToBlock(rp.block().getBlknum() + 1);
            currentslot = rp.nextAfter(currentslot);
        }
        return true;
    }

    @Override
    public int getAsInt(String field) {
        return rp.getInt(currentslot, field);
    }

    @Override
    public String getAsString(String field) {
        return rp.getString(currentslot, field);
    }

    @Override
    public Constant getVal(String field) {
        if (layout.schema().type(field) == INTEGER){
            return new Constant(getAsInt(field));
        }else{
            return new Constant(getAsString(field));
        }
    }

    @Override
    public boolean hasField(String field) {
        return layout.schema().hasField(field);
    }

    @Override
    public void close() {
        if(rp != null){
            tx.unpin(rp.block());
        }
    }
}
