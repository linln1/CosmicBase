package index.btree;

import file.Block;
import mvcc.Transaction;
import query.Constant;
import record.Layout;

public class BTDir {
    private Transaction tx;
    private Layout layout;
    private BTPage contents;
    private String filename;

    public BTDir(Transaction tx, Block blk, Layout layout){
        this.tx = tx;
        this.layout = layout;
        contents = new BTPage(tx, blk, layout);
        filename = blk.getFilename();
    }

    public void close(){
        contents.close();
    }

    public int search(Constant searchKey){
        Block childblk = findChildBlk(searchKey);
        while(contents.getFlag() > 0){
            contents.close();
            contents = new BTPage(tx, childblk, layout);
            childblk = findChildBlk(searchKey);
        }
        return childblk.getBlknum();
    }

    private Block findChildBlk(Constant searchKey) {
        int slot = contents.findSlotBefore(searchKey);
        if (contents.getDataVal(slot+1).equals(searchKey)) {
            slot++;
        }
        int blknum = contents.getChildNum(slot);
        return new Block(filename, blknum);
    }

    public void makeNewRoot(DirEntry e){
        Constant firstval = contents.getDataVal(0);
        int level = contents.getFlag();
        Block newblk = contents.split(0, level); //ie, transfer all the records
        DirEntry oldroot = new DirEntry(firstval, newblk.getBlknum());
        insertEntry(oldroot);
        insertEntry(e);
        contents.setFlag(level+1);
    }

    public DirEntry insert(DirEntry e) {
        if (contents.getFlag() == 0) {
            return insertEntry(e);
        }
        Block childblk = findChildBlk(e.getDataval());
        BTDir child = new BTDir(tx, childblk, layout);
        DirEntry myentry = child.insert(e);
        child.close();
        return (myentry != null) ? insertEntry(myentry) : null;
    }

    private DirEntry insertEntry(DirEntry e) {
        int newslot = 1 + contents.findSlotBefore(e.getDataval());
        contents.insertDir(newslot, e.getDataval(), e.getBlocknum());
        if (!contents.isFull())
            return null;
        // else page is full, so split it
        int level = contents.getFlag();
        int splitpos = contents.getNumRecs() / 2;
        Constant splitval = contents.getDataVal(splitpos);
        Block newblk = contents.split(splitpos, level);
        return new DirEntry(splitval, newblk.getBlknum());
    }
}
