package index.btree;

import file.Block;
import mvcc.Transaction;
import query.Constant;
import record.Layout;
import record.RId;

/**
 * 维护B-tree页块的内容
 */
public class BTLeaf {

    private Transaction tx;
    private Layout layout;
    private Constant searchKey;
    private BTPage contents;
    private int currentslot;
    private String filename;

    /**
     * 打开一个缓冲区来维护特定的叶子块，缓冲区在record拥有特定的searchkey之前被立即定位
     */
    public BTLeaf(Transaction tx, Block blk, Layout layout, Constant searchKey){
        this.tx = tx;
        this.layout = layout;
        this.searchKey = searchKey;
        contents = new BTPage(tx, blk, layout);
        currentslot = contents.findSlotBefore(searchKey);
        filename = blk.getFilename();
    }

    public void close() {
        contents.close();
    }

    public boolean next(){
        currentslot++;
        if(currentslot >= contents.getNumRecs()){
            return tryOverFlow();
        }
        else if(contents.getDataVal(currentslot).equals(searchKey)){
            return true;
        }else{
            return tryOverFlow();
        }
    }

    public RId getDataRid() {
        return contents.getDataRId(currentslot);
    }

    public void delete(RId datarid) {
        while(next()) {
            if(getDataRid().equals(datarid)) {
                contents.delete(currentslot);
                return;
            }
        }
    }

    public DirEntry insert(RId datarid) {
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchKey) > 0) {
            Constant firstval = contents.getDataVal(0);
            Block newblk = contents.split(0, contents.getFlag());
            currentslot = 0;
            contents.setFlag(-1);
            contents.insertLeaf(currentslot, searchKey, datarid);
            return new DirEntry(firstval, newblk.getBlknum());
        }

        currentslot++;
        contents.insertLeaf(currentslot, searchKey, datarid);
        if (!contents.isFull())
            return null;
        // else page is full, so split it
        Constant firstkey = contents.getDataVal(0);
        Constant lastkey  = contents.getDataVal(contents.getNumRecs()-1);
        if (lastkey.equals(firstkey)) {
            // create an overflow block to hold all but the first simpledb.record
            Block newblk = contents.split(1, contents.getFlag());
            contents.setFlag(newblk.getBlknum());
            return null;
        }
        else {
            int splitpos = contents.getNumRecs() / 2;
            Constant splitkey = contents.getDataVal(splitpos);
            if (splitkey.equals(firstkey)) {
                // move right, looking for the next key
                while (contents.getDataVal(splitpos).equals(splitkey)) {
                    splitpos++;
                }
                splitkey = contents.getDataVal(splitpos);
            }
            else {
                // move left, looking for first entry having that key
                while (contents.getDataVal(splitpos-1).equals(splitkey)) {
                    splitpos--;
                }
            }
            Block newblk = contents.split(splitpos, -1);
            return new DirEntry(splitkey, newblk.getBlknum());
        }
    }

    private boolean tryOverFlow() {
        Constant firstkey = contents.getDataVal(0);
        int flag = contents.getFlag();
        if (!searchKey.equals(firstkey) || flag < 0) {
            return false;
        }
        contents.close();
        Block nextblk = new Block(filename, flag);
        contents = new BTPage(tx, nextblk, layout);
        currentslot = 0;
        return true;
    }

}
