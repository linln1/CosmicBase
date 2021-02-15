package mvcc.concur;

import file.Block;

import java.util.HashMap;
import java.util.Map;


/**
 * 锁定表，它提供了锁定和解锁块的方法。如果一个事务请求一个导致与现有锁冲突的锁，
 * 那么该事务将被放在一个等待列表中。所有块只有一个等待列表。
 * 当一个块上的最后一个锁被解锁时，所有事务将从等待列表中删除并重新调度
 * 如果其中一个事务发现它正在等待的锁仍然是锁着的，它将把自己放回到等待列表中。
 */
public class LockTable {
    private static final long MAX_TIME = 10000; // 10 seconds

    private Map<Block,Integer> locks = new HashMap<Block,Integer>();

    /**
     * 给块一个SLock,如果这个块本来就有一个XLock，那么将放入等待列表直到解锁
     * 如果这个事务线程等待的时间超出10s，就抛出异常
     */
    public synchronized void sLock(Block blk){
        try{
            long timestamp = System.currentTimeMillis();
            while(hasXLock(blk) && !OverTime(timestamp)){
                wait(MAX_TIME);
            }
            if(hasXLock(blk)){
                throw new LockAbortException();
            }
            int val = getLockVal(blk);
            locks.put(blk, val+ 1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void xLock(Block blk){
        try {
            long timestamp = System.currentTimeMillis();
            while(hasOtherSLocks(blk) && !OverTime(timestamp)){
                wait(MAX_TIME);
            }
            if(hasOtherSLocks(blk)){
                throw new LockAbortException();
            }
            locks.put(blk, -1);
        } catch (InterruptedException e) {
            throw new LockAbortException();
        }
    }

    public synchronized void unlock(Block blk){
        int val = getLockVal(blk);
        if(val > 1){
            locks.put(blk, val-1);
        }else{
            locks.remove(blk);
            notifyAll();
        }
    }

    private boolean hasOtherSLocks(Block blk) {
        return getLockVal(blk) > 1;
    }

    private boolean OverTime(long timestamp) {
        return System.currentTimeMillis() - timestamp > MAX_TIME;
    }

    private boolean hasXLock(Block blk) {
        return getLockVal(blk) < 0;
    }

    private int getLockVal(Block blk) {
        Integer ival = locks.get(blk);
        return (ival == null) ? 0 : ival.intValue();
    }
}
