package index.btree;

import file.Block;
import index.Index;
import mvcc.Transaction;
import plan.Impl.IndexUpdatePlanner;
import query.Constant;
import record.Layout;
import record.RId;
import record.Schema;

import static java.sql.Types.INTEGER;

public class BTIndex implements Index {
    private Transaction tx;
    private Layout dirLayout, leafLayout;
    private String leafTbl;
    private BTLeaf leaf = null;
    private Block rootblk;

    public BTIndex(Transaction tx, String idxname, Layout leafLayout){
        this.tx = tx;
        // deal with the leaves
        leafTbl = idxname + "leaf";
        this.leafLayout = leafLayout;
        if (tx.size(leafTbl) == 0) {
            Block blk = tx.append(leafTbl);
            BTPage node = new BTPage(tx, blk, leafLayout);
            node.format(blk, -1);
        }

        // deal with the directory
        Schema dirsch = new Schema();
        dirsch.add("block",   leafLayout.schema());
        dirsch.add("dataval", leafLayout.schema());
        String dirtbl = idxname + "dir";
        dirLayout = new Layout(dirsch);
        rootblk = new Block(dirtbl,
                0);
        if (tx.size(dirtbl) == 0) {
            // create new root block
            tx.append(dirtbl);
            BTPage node = new BTPage(tx, rootblk, dirLayout);
            node.format(rootblk, 0);
            // insert initial directory entry
            int fldtype = dirsch.type("dataval");
            Constant minval = (fldtype == INTEGER) ?
                    new Constant(Integer.MIN_VALUE) :
                    new Constant("");
            node.insertDir(0, minval, 0);
            node.close();
        }
    }

    @Override
    public void startPtr(Constant searchkey) {
        close();
        BTDir root = new BTDir(tx, rootblk, dirLayout);
        int blknum = root.search(searchkey);
        root.close();
        Block leafblk = new Block(leafTbl, blknum);
        leaf = new BTLeaf(tx, leafblk, leafLayout, searchkey);
    }

    @Override
    public boolean nextPtr() {
        return leaf.next();
    }

    @Override
    public RId getDataRId() {
        return leaf.getDataRid();
    }

    @Override
    public void insert(Constant dataval, RId datarid) {
        startPtr(dataval);
        DirEntry e = leaf.insert(datarid);
        leaf.close();
        if (e == null) {
            return;
        }
        BTDir root = new BTDir(tx, rootblk, dirLayout);
        DirEntry e2 = root.insert(e);
        if (e2 != null) {
            root.makeNewRoot(e2);
        }
        root.close();
    }

    @Override
    public void delete(Constant dataval, RId datarid) {
        startPtr(dataval);
        leaf.delete(datarid);
        leaf.close();
    }

    @Override
    public void close() {
        if(leaf != null){
            leaf.close();
        }
    }

    public static int searchCost(int numblocks, int rpb) {
        return 1 + (int)(Math.log(numblocks) / Math.log(rpb));
    }

}
