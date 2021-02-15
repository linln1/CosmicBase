package record;

import file.Page;

import java.util.HashMap;
import java.util.Map;
import static java.sql.Types.*;


/**
 * 封装一个schema，用offsets分别记录schema中fileds对应的物理偏置，总共需要slotsize大小来记录
 */
public class Layout {

    private Schema schema;
    private Map<String, Integer> offsets;
    private int slotsize;

    /**
     * 决定每个field的物理offset
     * @param schema
     */
    public Layout(Schema schema){
        this.schema = schema;
        offsets = new HashMap<>();
        int pos = Integer.BYTES;
        for (String fldname : schema.fields()){
            offsets.put(fldname, pos);
            pos += lengthInBytes(fldname);
        }
        slotsize = pos;
    }

    /**
     * 从目录中检索元数据
     * @param schema
     * @param offsets
     * @param slotsize
     */
    public Layout(Schema schema, Map<String, Integer> offsets, int slotsize){
        this.schema    = schema;
        this.offsets   = offsets;
        this.slotsize = slotsize;
    }

    public Schema schema(){
        return schema;
    }

    public synchronized int offset(String fldname) {return offsets.get(fldname);}

    public synchronized int slotSize() {return slotsize;}

    private int lengthInBytes(String fldname){
        int fldtype = schema.type(fldname);
        if(fldtype == INTEGER) {
            return Integer.BYTES;
        } else {
            return Page.maxLength(schema.length(fldname));
        }
    }
}
