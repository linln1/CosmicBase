package excutor.impl;

import excutor.Scan;
import file.Block;
import mvcc.Transaction;
import query.Constant;
import record.Layout;
import record.RecordPage;

import java.util.ArrayList;
import java.util.List;

import static java.sql.Types.INTEGER;

/**
 * for chunk operator
 */
public class ChunkScan implements Scan {
    private List<RecordPage> buffs = new ArrayList<>();
    private Transaction tx;
    private String filename;
    private RecordPage rp;
    private Layout layout;
    private int startbnum, endbnum, currentbnum;
    private int currentslot;

    public ChunkScan(Transaction tx, String filename, Layout layout, int startbnum, int endbnum) {
        this.tx = tx;
        this.filename = filename;
        this.layout = layout;
        this.startbnum = startbnum;
        this.endbnum   = endbnum;
        for (int i=startbnum; i<=endbnum; i++) {
            Block blk = new Block(filename, i);
            buffs.add(new RecordPage(tx, blk, layout));
        }
        moveToBlock(startbnum);
    }

    private void moveToBlock(int blknum) {
        currentbnum = blknum;
        rp = buffs.get(currentbnum - startbnum);
        currentslot = -1;
    }

    @Override
    public void StartPtr() {
        moveToBlock(startbnum);
    }

    @Override
    public boolean nextPtr() {
        currentslot = rp.nextAfter(currentslot);
        while (currentslot < 0) {
            if (currentbnum == endbnum) {
                return false;
            }
            moveToBlock(rp.block().getBlknum()+1);
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
        if (layout.schema().type(field) == INTEGER) {
            return new Constant(getAsInt(field));
        } else {
            return new Constant(getAsString(field));
        }
    }

    @Override
    public boolean hasField(String field) {
        return layout.schema().hasField(field);
    }

    @Override
    public void close() {
        for (int i = 0 ; i < buffs.size() ; i++ ){
            Block blk = new Block(filename, startbnum + 1);
            tx.unpin(blk);
        }
    }
}
