package mvcc;

import buffer.Buffer;
import buffer.BufferManager;
import file.Block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manage the transaction's currently-pinned buffers
 */
public class BufferList {
    private Map<Block, Buffer> buffers = new HashMap<>();
    private List<Block> pins = new ArrayList<>();
    private BufferManager bm;

    public BufferList(BufferManager bm){
        this.bm = bm;
    }

    Buffer getBuffer(Block blk) {
        return buffers.get(blk);
    }

    void setPin(Block blk){
        Buffer buff = bm.pin(blk);
        buffers.put(blk, buff);
        pins.add(blk);
    }

    void unpin(Block blk){
        Buffer buff = buffers.get(blk);
        bm.pinClear(buff);
        pins.remove(blk);
        if(!pins.contains(blk)){
            buffers.remove(blk);
        }
    }

    void unpinAll(){
        for(Block blk: pins){
            Buffer buff = buffers.get(blk);
            bm.pinClear(buff);
        }
        buffers.clear();
        pins.clear();
    }



}
