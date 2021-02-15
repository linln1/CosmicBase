package metadata;

import index.Index;
import index.hash.HashIndex;
import mvcc.Transaction;
import record.Layout;
import record.Schema;

import static java.sql.Types.INTEGER;

/**
 * 这个信息是被用在planner中，被用来估计使用index的开销
 * 并且获取records的layout
 * 方法本质上和plan一样
 */
public class MetaIndex {
    private String idxname, fldname;
    private Transaction tx;
    private Schema tblSchema;
    private Layout idxLayout;
    private MetaStat ms;

    public MetaIndex(String idxname, String fldname, Schema tblSchema,
                     Transaction tx,  MetaStat ms) {
        this.idxname = idxname;
        this.fldname = fldname;
        this.tx = tx;
        this.tblSchema = tblSchema;
        this.idxLayout = createIdxLayout();
        this.ms = ms;
    }

    /**
     * 打开object索引
     * @return
     */
    public Index open() {
        return new HashIndex(tx, idxname, idxLayout);
//    return new BTreeIndex(simpledb.tx, idxname, idxLayout);
    }

    /**
     * 估计有特定searchkey的记录占用的块的个数
     * 这个方法使用表的metadata来估计index文件的大小以及每个块上indexrecord的个数
     * 然后它将这个信息传递给travelcost
     * @return
     */
    public int getBlockNum(){
        int rpb = tx.blockSize() / idxLayout.slotSize();
        int Num = ms.getNumRecs();
        return HashIndex.searchCost(Num, rpb);
    }

    /**
     * 返回估计的有searchkey的records的条数
     * 这个值与做一个select查询是一样的，那就是，它是表中记录的数量
     * 除以索引字段的不同值的数目
     */
    public int getNumRecs(){
        return ms.getNumRecs() / ms.getDistinctValues(fldname);
    }

    public int getDistinctValues(String fname){
        return fldname.equals(fname) ? 1 : ms.getDistinctValues(fldname);
    }

    private Layout createIdxLayout() {
        Schema sch = new Schema();
        sch.addIntField("block");
        sch.addIntField("id");
        if (tblSchema.type(fldname) == INTEGER) {
            sch.addIntField("dataval");
        } else {
            int fldlen = tblSchema.length(fldname);
            sch.addStringField("dataval", fldlen);
        }
        return new Layout(sch);
    }

}
