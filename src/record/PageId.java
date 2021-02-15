package record;

/**
 * 用PageId来记录指定块中的特定位置
 */
public class PageId {
    private int Id;
    private int slot;

    /**
     * Create a PageID for the record having the
     * specified location in the specified block
     * @param Id the block number where the record lives
     * @param Slot the record's location
     */
    public PageId(int Id, int Slot){
        this.Id = Id;
        this.slot = Slot;
    }

    public synchronized int getPageId(){
        return this.Id;
    }

    public synchronized int getSlot(){
        return this.slot;
    }

    @Override
    public boolean equals(Object o){
        if(o == null){
            return false;
        }
        if( o.getClass() != this.getClass()){
            return false;
        }
        else{
            PageId id = (PageId) o;
            return id.Id == this.Id && id.slot == this.slot;
        }
    }

    @Override
    public String toString(){
        return "[" + Id + ", " + slot + "]";
    }
}
