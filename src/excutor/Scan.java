package excutor;

import parse.Constant;

/**
 * The interface is the abstract Class of the other scanner
 * It's the relationship algebra operator4
 * @author linln
 */

public interface Scan {

    public void StartPtr();

    public boolean nextPtr();

    public int getAsInt(String field);

    public String getAsString(String field);

    public Constant getVal(String field);

    public boolean hasField(String field);

    public void close();
}
