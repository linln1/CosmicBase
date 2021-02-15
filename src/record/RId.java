package record;

/**
 * 有一个标识符用来标记文件中的record
 * 一个RId是由文件的块号和record在那个块中的位置
 */
public class RId {
    private int blknum;
    private int slot;

    public RId(int blknum, int slot) {
        this.blknum = blknum;
        this.slot = slot;
    }

    public int getBlkNum(){
        return blknum;
    }

    public int getSlot(){
        return slot;
    }

    @Override
    public boolean equals(Object obj){
        if(obj == null){
            return false;
        }else{
            RId r = (RId) obj;
            return blknum == r.blknum && slot == r.slot;
        }
    }

    @Override
    public String toString() {
        return "[ " + blknum + ", " + slot + " ]";
    }
}
