package excutor;

import query.Constant;
import record.PageId;
import record.RId;
import sun.plugin2.main.server.ResultID;

public interface UpdateScan extends Scan{


    public void setInt(String field, int val);

    public void setString(String field, String val);

    public void setConstant(String field, Constant val);

    public void insert();

    public void delete();

    public RId getRid();

    public void moveToRid(RId rid);
}
