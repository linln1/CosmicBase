package index;

import query.Constant;
import record.RId;

/**
 * 包含用来遍历Index的方法
 */
public interface Index {

    public void startPtr(Constant searchkey);

    public boolean nextPtr();

    public RId getDataRId();

    public void insert(Constant dataval, RId datarid);

    public void delete(Constant dataval, RId datarid);

    public void close();
}
