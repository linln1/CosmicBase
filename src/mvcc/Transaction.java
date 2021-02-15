package mvcc;

import buffer.Buffer;
import buffer.BufferAbortException;
import buffer.BufferManager;
import file.Block;
import file.FileManager;
import file.Page;
import log.LogFileManager;
import mvcc.concur.ConcurrencyManager;
import mvcc.recover.RecoveryManager;

/**
 *
 */
public class Transaction {
    private static int nextTxNum = 0;
    private static final int END_OF_FILE = -1;
    private RecoveryManager recoveryMgr;
    private ConcurrencyManager concurMgr;
    private BufferManager bm;
    private FileManager fm;

    private int txNum;
    private BufferList mybuffer;

    public Transaction(FileManager fm, LogFileManager lfm, BufferManager bm){
        this.fm = fm;
        this.bm = bm;
        txNum = nextTxNum();
        recoveryMgr = new RecoveryManager(this, txNum, lfm, bm);
        concurMgr = new ConcurrencyManager();
        mybuffer = new BufferList(bm);
    }

    private static synchronized int nextTxNum() {
        nextTxNum++;
        return nextTxNum;
    }

    public void commit(){
        recoveryMgr.commit();
        System.out.println("transaction " + txNum + " committed");
        concurMgr.release();
        mybuffer.unpinAll();
    }

    public void rollback(){
        recoveryMgr.rollback();
        System.out.println("transaction " + txNum + " rolled back");
        concurMgr.release();
        mybuffer.unpinAll();
    }

    public void recover(){
        bm.flushAll(txNum);
        recoveryMgr.recover();
    }

    public void pin(Block blk){
        mybuffer.setPin(blk);
    }

    public void unpin(Block blk){
        mybuffer.unpin(blk);
    }

    public int getInt(Block blk, int offset){
        concurMgr.sLock(blk);
        Buffer buff = mybuffer.getBuffer(blk);
        return buff.getPage().getInt(offset);
    }

    public String getString(Block blk, int offset){
        concurMgr.sLock(blk);
        Buffer buff = mybuffer.getBuffer(blk);
        return buff.getPage().getString(offset);
    }

    public synchronized void setInt(Block blk, int offset, int val, boolean okToLog){
        concurMgr.xLock(blk);
        Buffer buff = mybuffer.getBuffer(blk);
        int lsn = -1;
        if(okToLog){
            lsn = recoveryMgr.setInt(buff, offset, val);
        }
        Page p = buff.getPage();
        p.setInt(offset, val);
        buff.setModified(txNum, lsn);
    }

    public synchronized void setString(Block blk, int offset, String val, boolean okToLog){
        concurMgr.xLock(blk);
        Buffer buff = mybuffer.getBuffer(blk);
        int lsn = -1;
        if(okToLog){
            lsn = recoveryMgr.setString(buff, offset, val);
        }
        Page p = buff.getPage();
        p.setString(offset, val);
        buff.setModified(txNum, lsn);
    }

    public synchronized int size(String field){
        Block dummyblk = new Block(field, END_OF_FILE);
        concurMgr.sLock(dummyblk);
        return fm.length(field);
    }

    public Block append(String filename){
        Block dummyblk = new Block(filename, END_OF_FILE);
        concurMgr.xLock(dummyblk);
        return fm.append(filename);
    }

    public synchronized int blockSize(){
        return fm.getBlocksize();
    }

    public synchronized int availableBuffers(){
        return bm.available();
    }


}
