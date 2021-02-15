package excutor.impl;

import buffer.BufferNeeds;
import excutor.Scan;
import mvcc.Transaction;
import query.Constant;
import record.Layout;

public class MultiBufferProductScan implements Scan {
    private Transaction tx;
    private Scan lhsScan, rhsScan, prodScan;
    private String filename;
    private Layout layout;
    private int chunkSize, nextBlkNum, fileSize;

    public MultiBufferProductScan(Transaction tx, Scan lhsScan, String filename, Layout layout){
        this.tx = tx;
        this.lhsScan = lhsScan;
        this.filename = filename;
        this.layout = layout;
        fileSize = tx.size(filename);
        int available = tx.availableBuffers();
        chunkSize = BufferNeeds.bestFactor(available, fileSize);
        StartPtr();
    }

    @Override
    public void StartPtr() {
        nextBlkNum = 0;
        useNextChunk();
    }

    private boolean useNextChunk() {
        if (rhsScan != null) {
            rhsScan.close();
        }
        if (nextBlkNum >= fileSize) {
            return false;
        }
        int end = nextBlkNum + chunkSize - 1;
        if (end >= fileSize) {
            end = fileSize - 1;
        }
        rhsScan = new ChunkScan(tx, filename, layout, nextBlkNum, end);
        lhsScan.StartPtr();
        prodScan = new ProductScan(lhsScan, rhsScan);
        nextBlkNum = end + 1;
        return true;
    }

    @Override
    public boolean nextPtr() {
        while(!prodScan.nextPtr()){
            if(!useNextChunk()){
                return false;
            }
        }
        return true;
    }

    @Override
    public int getAsInt(String field) {
        return prodScan.getAsInt(field);
    }

    @Override
    public String getAsString(String field) {
        return prodScan.getAsString(field);
    }

    @Override
    public Constant getVal(String field) {
        return prodScan.getVal(field);
    }

    @Override
    public boolean hasField(String field) {
        return prodScan.hasField(field);
    }

    @Override
    public void close() {
        prodScan.close();
    }
}
