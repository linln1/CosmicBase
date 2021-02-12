package record;

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

    public int getPageId(){
        return this.Id;
    }

    public int getSlot(){
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

    public String toString(){
        return "[" + Id + ", " + slot + "]";
    }
}
