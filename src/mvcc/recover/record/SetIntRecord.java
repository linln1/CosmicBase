package mvcc.recover.record;

import file.Block;
import file.Page;
import log.LogFileManager;
import mvcc.Transaction;

public class SetIntRecord implements LogRecord{
    private int txNum, offset, val;
    private Block blk;

    public SetIntRecord(Page p){
        int pos =Integer.BYTES;
        txNum = p.getInt(pos);
        int fpos = pos + Integer.BYTES;
        String filename = p.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);
        blk = new Block(filename, blknum);
        int opos = bpos + Integer.BYTES;
        offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        val = p.getInt(vpos);
    }



    @Override
    public int getType() {
        return SETINT;
    }

    @Override
    public int getTxNum() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setInt(blk, offset, val, false);
        tx.unpin(blk);
    }

    @Override
    public String toString(){
        return "<SETINT " + txNum + " " + blk + " " + offset + " " + val + ">";
    }

    public static int writeToLog(LogFileManager lfm, int txNum, Block blk, int offset, int val) {
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blk.getFilename().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        byte[] rec = new byte[vpos + Integer.BYTES];
        Page p = new Page(rec);
        p.setInt(0, SETINT);
        p.setInt(tpos, txNum);
        p.setString(fpos, blk.getFilename());
        p.setInt(bpos, blk.getBlknum());
        p.setInt(opos, offset);
        p.setInt(vpos, val);
        return lfm.append(rec);
    }

}
