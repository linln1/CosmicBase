package excutor;

public class ProductScan {
    private Scan s1, s2;

    /**
     * Create a prodScan having two undering scans.
     * s1 && s2
     */
    public ProductScan(Scan s1, Scan s2){
        this.s1 = s1;
        this.s2 = s2;
        initScan();
    }

    public void initScan(){
        s1.StartPtr();
        s1.nextPtr();
        s2.StartPtr();
    }

    /**
     * Move the Scan to the next record.
     * The method moves to the next RHS Record, if possible.
     * Otherwise, it moves to the next LHS record and the
     * first RHS record.
     * If there are no more LHS records, the method return false.
     */
    public boolean next(){
        if(s2.nextPtr())
            return true;
        else{
            s2.StartPtr();
            return s2.nextPtr() && s1.nextPtr();
        }
    }

    /**
     * Return the integer value of the specified field.
     * The value is obtained from whichever scanner
     * contains the field.
     * @See Scan#getAsInt(String)
     */
    public int getAsInt(String field){
        if(s1.hasField(field)){
            return s1.getAsInt(field);
        }else{
            return s2.getAsInt(field);
        }
    }
    /**
     * Return the String value of the specified field.
     * The value is obtained from whichever scanner
     * contains the field.
     * @See Scan#getAsString(String)
     */
    public String getAsString(String field){
        if(s1.hasField(field)){
            return s1.getAsString(field);
        }
        else{
            return s2.getAsString(field);
        }
    }
    /**
     * Return the Constant value of the specified field.
     * The value is obtained from whichever scanner
     * contains the field.
     * @See Scan#getAsConstant(String)
     */
    public String getAsConstant(String field){
        if(s1.hasField(field)){
            return s1.getAsString(field);
        }
        else{
            return s2.getAsString(field);
        }
    }

    /**
     * Returns true if the specified field is in
     * either of the underlying scans.
     */
    public boolean hasField(String field){
        return s1.hasField(field) || s2.hasField(field);
    }

    /**
     * Close the scans
     * @see Scan# cleanScan()
     */
    public void cleanScan(){
        s1.close();
        s2.close();
    }
}
