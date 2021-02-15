package mvcc.concur;

import file.Block;

import java.util.HashMap;
import java.util.Map;

/***
 * 每一个事务都有自己的并发管理器，保证追踪目前事务拥有的锁，并且和全局锁表进行交互
 */
public class ConcurrencyManager {

    private static LockTable locktbl = new LockTable();
    private Map<Block, String> locks = new HashMap<>();

    public void sLock(Block blk){
        if(locks.get(blk) == null){
            locktbl.sLock(blk);
            locks.put(blk, "S");
        }
    }

    /**
     * 如果没有XLock，首先获取SLOCK，然后升级成XLOCK
     * @param blk
     */
    public void xLock(Block blk){
        if(!hasXLock(blk)){
            sLock(blk);
            locktbl.xLock(blk);
            locks.put(blk, "X");
        }
    }

    /**
     * 释放所有的锁通过查询锁表来解锁每一个锁
     */
    public void release(){
        for (Block blk : locks.keySet()){
            locktbl.unlock(blk);
        }
        locks.clear();
    }

    private boolean hasXLock(Block blk){
        String locktype = locks.get(blk);
        return locktype != null && locktype.equals("X");
    }


}
