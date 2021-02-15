package mvcc.recover;

import buffer.Buffer;
import buffer.BufferManager;
import file.Block;
import log.LogFileManager;
import mvcc.Transaction;
import mvcc.recover.record.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

public class RecoveryManager {

    private LogFileManager lfm;
    private BufferManager bm;
    private Transaction tx;
    private int txNum;


    public RecoveryManager(Transaction tx, int txNum, LogFileManager lfm, BufferManager bm){
        this.tx = tx;
        this.txNum = txNum;
        this.lfm = lfm;
        this.bm = bm;
        StartRecord.writeToLog(lfm, txNum);
    }

    public void commit(){
        bm.flushAll(txNum);
        int lsn = CommitRecord.writeToLog(lfm, txNum);
        lfm.flush(lsn);
    }


    public void rollback(){
        doRollback();
        bm.flushAll(txNum);
        int lsn = RollbackRecord.writeToLog(lfm, txNum);
        lfm.flush(lsn);
    }

    /**
     * rollback till find the START record
     */
    private void doRollback() {
        Iterator<byte[]> iter = lfm.iterator();
        while (iter.hasNext()) {
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if (rec.getTxNum() == txNum) {
                if (rec.getType() == LogRecord.START) {
                    return;
                }
                rec.undo(tx);
            }
        }
    }

    public void recover() {
        doRecover();
        bm.flushAll(txNum);
        int lsn = CheckpointRecord.writeToLog(lfm);
        lfm.flush(lsn);
    }

    private void doRecover() {
        Collection<Integer> finishedTxs = new ArrayList<>();
        Iterator<byte[]> iter = lfm.iterator();
        while(iter.hasNext()){
            byte[] bytes = iter.next();
            LogRecord rec = LogRecord.createLogRecord(bytes);
            if(rec.getType() == LogRecord.CHECKPOINT){
                return ;
            }if (rec.getType() == LogRecord.COMMIT || rec.getType() == LogRecord.ROLLBACK){
                finishedTxs.add(rec.getTxNum());
            }else if(!finishedTxs.contains(rec.getTxNum())){
                rec.undo(tx);
            }
        }
    }

    public int setInt(Buffer buff, int offset, int newval) {
        int oldval = buff.getPage().getInt(offset);
        Block blk = buff.getBlk();
        return SetIntRecord.writeToLog(lfm, txNum, blk, offset, oldval);
    }

    public int setString(Buffer buff, int offset, String newval){
        String oldval = buff.getPage().getString(offset);
        Block blk = buff.getBlk();
        return SetStringRecord.writeToLog(lfm, txNum, blk, offset, oldval);
    }


}
