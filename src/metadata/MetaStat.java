package metadata;

/**
 * 维护每张表的简单统计信息
 * 块的个数，记录的个数，每个域不同的值数
 */
public class MetaStat {
    private int numBlocks;
    private int numRecs;

    public MetaStat(int numblocks, int numRecs) {
        this.numBlocks = numblocks;
        this.numRecs   = numRecs;
    }

    public int getNumBlocks() {
        return numBlocks;
    }

    public int getNumRecs() {
        return numRecs;
    }

    public int getDistinctValues(String fldname) {
        return 1 + (numRecs / 3);
    }
}
