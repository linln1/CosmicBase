package excutor.impl;


import excutor.Scan;
import query.Constant;

public class ProductScan implements Scan {
    private Scan s1, s2;

    public ProductScan(Scan s1, Scan s2){
        this.s1 = s1;
        this.s2 = s2;
        StartPtr();
    }

    @Override
    public void StartPtr() {
        s1.StartPtr();
        s1.nextPtr();
        s2.StartPtr();
    }

    @Override
    public boolean nextPtr() {
        if(s2.nextPtr()){
            return true;
        }else{
            s2.StartPtr();
            return s2.nextPtr() && s1.nextPtr();
        }
    }

    @Override
    public int getAsInt(String field) {
        if (s1.hasField(field)) {
            return s1.getAsInt(field);
        } else {
            return s2.getAsInt(field);
        }
    }

    @Override
    public String getAsString(String field) {
        if (s1.hasField(field)) {
            return s1.getAsString(field);
        } else {
            return s2.getAsString(field);
        }
    }

    @Override
    public Constant getVal(String field) {
        if (s1.hasField(field)) {
            return s1.getVal(field);
        } else {
            return s2.getVal(field);
        }
    }

    @Override
    public boolean hasField(String field) {
        return s1.hasField(field) || s2.hasField(field);
    }

    @Override
    public void close() {
        s1.close();
        s2.close();
    }
}