package mvcc.recover.record;

import file.Block;
import file.Page;
import log.LogFileManager;
import mvcc.Transaction;
import sun.rmi.log.ReliableLog;


/**
 * Record 的格式
 * <SETSTRING> <txNum> <blk's filename> <blk's number> <offset> <val>
 */
public class SetStringRecord implements LogRecord{

    private int txNum, offset;
    private String val;
    private Block blk;

    public SetStringRecord(Page p){
        int tpos = Integer.BYTES;
        txNum = p.getInt(tpos);
        int fpos = tpos + Integer.BYTES;
        String filename = p.getString(fpos);
        int bpos = fpos + Page.maxLength(filename.length());
        int blknum = p.getInt(bpos);
        blk = new Block(filename, blknum);
        int opos = bpos + Integer.BYTES;
        offset = p.getInt(opos);
        int vpos = opos + Integer.BYTES;
        val = p.getString(vpos);
    }


    @Override
    public int getType() {
        return SETSTRING;
    }

    @Override
    public int getTxNum() {
        return txNum;
    }

    @Override
    public void undo(Transaction tx) {
        tx.pin(blk);
        tx.setString(blk, offset, val, false);
        tx.unpin(blk);
    }

    @Override
    public String toString(){
        return "<SETSTRING " + txNum + " " + blk + " " + offset + " " + val + ">";
    }

    public static int writeToLog(LogFileManager lfm,int txNum, Block blk, int offset, String val){
        int tpos = Integer.BYTES;
        int fpos = tpos + Integer.BYTES;
        int bpos = fpos + Page.maxLength(blk.getFilename().length());
        int opos = bpos + Integer.BYTES;
        int vpos = opos + Integer.BYTES;
        int reclen = vpos + Page.maxLength(val.length());

        byte[] rec = new byte[reclen];
        Page p = new Page(rec);
        p.setInt(0, SETSTRING);
        p.setInt(tpos, txNum);
        p.setString(fpos, blk.getFilename());
        p.setInt(bpos, blk.getBlknum());
        p.setInt(opos, offset);
        p.setString(vpos, val);
        return lfm.append(rec);
    }

}
