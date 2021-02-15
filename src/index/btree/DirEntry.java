package index.btree;

import query.Constant;

/**
 * 一个目录entry有两个成分
 * childblock的个数
 * 那个块中第一条record的dataval
 */
public class DirEntry {
    private Constant dataval;
    private int blocknum;

    public DirEntry(Constant dataval, int blocknum){
        this.dataval = dataval;
        this.blocknum = blocknum;
    }

    public Constant getDataval(){
        return dataval;
    }

    public int getBlocknum(){
        return blocknum;
    }
}
