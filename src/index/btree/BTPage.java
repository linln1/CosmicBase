package index.btree;

import file.Block;
import mvcc.Transaction;
import query.Constant;
import record.Layout;

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
        while (slot < getNumRec() && getDataVal(slot).compareTo(searchKey) < 0){
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


}
