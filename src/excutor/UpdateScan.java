package excutor;

import parse.Constant;
import record.PageId;

public interface UpdateScan extends Scan{

    /**
     * Modify the field value of the current record.
     * @param field the name of the field
     * @param val the new value, expressed as a Int
     */
    public void setInt(String field, Constant val);

    /**
     * Modify the field value of the current record.
     * @param field the name of the field
     * @param val the new value, expressed as a String
     */
    public void setString(String field, Constant val);

    /**
     * Modify the field value of the current record.
     * @param field the name oft he filed
     * @param val the new value, expressed as a Constant
     */
    public void setConstant(String field, Constant val);

    /**
     * Insert a new record somewhere in the scanner.
     */
    public void insert();

    /**
     * Delete the current record from the scanner.
     */
    public void delete();

    /**
     * Return the id of the current record.
     * @return the id of the current record
     */
    public PageId getRid();

    /**
     * Position the scan so that the current record has
     * the specified id.
     * @param rid the id of the desired record
     */
    public void moveToRid(PageId rid);
}
