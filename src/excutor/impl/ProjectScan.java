package excutor.impl;

import excutor.Scan;
import query.Constant;

import java.util.List;

public class ProjectScan implements Scan{
    private Scan s;
    private List<String> fieldList;

    public ProjectScan(Scan s, List<String> fields) {
        this.s = s;
        this.fieldList = fields;
    }

    @Override
    public void StartPtr() {
        s.StartPtr();
    }

    @Override
    public boolean nextPtr() {
        return s.nextPtr();
    }

    @Override
    public int getAsInt(String field) {
        if (hasField(field)) {
            return s.getAsInt(field);
        } else {
            throw new RuntimeException("field " + field + " not found.");
        }
    }

    @Override
    public String getAsString(String field) {
        if (hasField(field)) {
            return s.getAsString(field);
        } else {
            throw new RuntimeException("field " + field + " not found.");
        }
    }

    @Override
    public Constant getVal(String field) {
        if (hasField(field)) {
            return s.getVal(field);
        } else {
            throw new RuntimeException("field " + field + " not found.");
        }
    }

    @Override
    public boolean hasField(String field) {
        return fieldList.contains(field);
    }

    @Override
    public void close() {
        s.close();
    }
}
