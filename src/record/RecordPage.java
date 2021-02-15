package record;

import file.Block;
import mvcc.Transaction;
import query.Constant;

import static java.sql.Types.INTEGER;

/**
 * 将record存储在块上的特定位置
 */
public class RecordPage {
    public static final int EMPTY = 0, USED = 1;
    private Transaction tx;
    private Block blk;
    private Layout layout;

    public RecordPage(Transaction tx, Block blk, Layout layout){
        this.tx = tx;
        this.blk = blk;
        this.layout = layout;
        tx.pin(blk);
    }

    public int getInt(int currentslot, String field) {
        int fldpos = offset(currentslot) + layout.offset(field);
        return tx.getInt(blk, fldpos);
    }

    public String getString(int currentslot, String field) {
        int fldpos = offset(currentslot) + layout.offset(field);
        return tx.getString(blk, fldpos);
    }

    public synchronized void setInt(int slot, String field, int val){
        int fldpos = offset(slot) + layout.offset(field);
        tx.setInt(blk, fldpos, val, true);
    }

    public synchronized void setString(int slot, String field, String val){
        int fldpos = offset(slot) + layout.offset(field);
        tx.setString(blk, fldpos, val, true);
    }

    public void delete(int slot) {setFlag(slot, EMPTY);}

    public void format() {
        int slot = 0;
        while (isValidSlot(slot)) {
            tx.setInt(blk, offset(slot), EMPTY, false);
            Schema sch = layout.schema();
            for (String fldname : sch.fields()) {
                int fldpos = offset(slot) + layout.offset(fldname);
                if (sch.type(fldname) == INTEGER) {
                    tx.setInt(blk, fldpos, 0, false);
                } else {
                    tx.setString(blk, fldpos, "", false);
                }
            }
            slot++;
        }
    }

    private int searchAfter(int slot, int flag) {
        slot++;
        while (isValidSlot(slot)) {
            if (tx.getInt(blk, offset(slot)) == flag) {
                return slot;
            }
            slot++;
        }
        return -1;
    }

    public int nextAfter(int slot){
        return searchAfter(slot, USED);
    }

    public Block block() {
        return blk;
    }

    private boolean isValidSlot(int slot) {
        return offset(slot+1) <= tx.blockSize();
    }

    private void setFlag(int slot, int flag) {
        tx.setInt(blk, offset(slot), flag, true);
    }

    private int offset(int slot) {
        return slot * layout.slotSize();
    }

    public int insertAfter(int slot) {
        int newslot = searchAfter(slot, EMPTY);
        if(newslot >= 0){
            setFlag(newslot, USED);
        }
        return newslot;
    }
}
