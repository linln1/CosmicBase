package buffer;

import file.*;
import log.*;

/**
 * Manages the buffers to blocks, implementation the alogrithm such as LRU
 * @author linln
 */
public class BufferManager {
    private Buffer[] bufpool;
    private int AvNum; // 空闲缓冲块的数目
    private static final long MAX_TIME = 10000; //10 seconds


    /**
     * A Constructor of the BufferManager
     * @param fm
     * @param lfm
     * @param bufNum
     */
    public BufferManager(FileManager fm, LogFileManager lfm, int bufNum){
        bufpool = new Buffer[bufNum];
        AvNum = bufNum;
        for(int i = 0; i < bufNum ; i++){
            bufpool[i] = new Buffer(fm, lfm);
        }
    }

    /**
     * Return the number of the available buffer
     * @return
     */
    public synchronized int available(){
        return AvNum;
    }

    /**
     * Commit all the dirty buffers modified by the specific transaction txnum
     * @param txnum
     */
    public synchronized void CommitAll(int txnum){
        for(Buffer buf: bufpool){
            if(buf.isModifiedBy(txnum)){
                buf.flush();
            }
        }
    }

    /**
     * clear the bin of the specific buf.
     * If its pincount goes to zero, then notifyAll the waiting threads.
     * @param buf the buffer should be clear the pin.
     */
    public synchronized void pinClear(Buffer buf){
        buf.pinsDec();
        if(!buf.isPinned()){
            AvNum++;
            notifyAll();
        }
    }

    /**
     * find the buf that store the blk
     * @param blk
     * @return
     */
    private Buffer find(Block blk){
        for(Buffer buf: bufpool){
            Block temp = buf.getBlk();
            if(temp != null && temp.equals(blk))
                return buf;
        }
        return null;
    }


    private Buffer chooseUnpinnedBuffer(){
        for(Buffer buf: bufpool){
            if(!buf.isPinned()){
                return buf;
            }
        }
        return null;
    }

    /**
     * try to pin a buf to store the specified block
     * if can't find it from the record, try to choose an unpinned buffer from
     * the pool to store it. Returns null if no available buffers.
     * @param blk
     * @return the pinned buffer
     */
    public Buffer tryToPin(Block blk){
        Buffer buf = find(blk);
        if(buf == null){
            buf = chooseUnpinnedBuffer();
            if(buf == null)
                return null;
            buf.assignToBlock(blk);
        }
        if(!buf.isPinned())
            AvNum--;
        buf.pinsInc();
        return buf;
    }

    /**
     * caculate the time differences of now and startTime
     * @param startTime
     * @return the time differences
     */
    private boolean waitTime(long startTime){
        return System.currentTimeMillis() - startTime > MAX_TIME;
    }

    /**
     * find a buf to store the blk
     * if no available buf exists, should wait until it free.
     * If no buffer becomes available within a fixed time period,
     * then a RuntimeException is thrown.
     * @param blk
     * @return the buffer pinned to the blk
     */
    public synchronized Buffer pin(Block blk){
        try{
            long timeStamp = System.currentTimeMillis();
            Buffer buf = tryToPin(blk);
            while(buf == null && !waitTime(timeStamp)){
                wait(MAX_TIME);
                buf = tryToPin(blk);
            }
            if(buf == null)
                throw new BufferAbortException();
            return buf;
        }catch (InterruptedException e){
            throw new BufferAbortException();
        }
    }



}
