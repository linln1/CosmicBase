package record;

import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import excutor.Scan;
import query.Constant;

import java.util.Comparator;
import java.util.List;

public class RecordComparator implements Comparator<Scan> {
    private List<String> fields;

    public RecordComparator(List<String> fields) { this.fields = fields;}

    @Override
    public int compare(Scan s1, Scan s2) {
        for (String fldname : fields){
            Constant v1 = s1.getVal(fldname);
            Constant v2 = s2.getVal(fldname);
            int result = v1.compareTo(v2);
            if(result != 0){
                return result;
            }
        }
        return 0;
    }

}
