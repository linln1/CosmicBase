package query;

import excutor.Scan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroupValue {
    private Map<String, Constant> vals  = new HashMap<>();

    public GroupValue(Scan s, List<String> fields){
        vals = new HashMap<String, Constant>();
        for (String fldname : fields){
            vals.put(fldname, (Constant) s.getVal(fldname));
        }
    }

    public Constant getVal(String fldname) {return vals.get(fldname) ; }

    @Override
    public boolean equals(Object obj){
        if(obj == null){
            return false;
        }
        else{
            GroupValue gv = (GroupValue) obj;
            for(String fldname : vals.keySet()){
                Constant v1 = vals.get(fldname);
                Constant v2 = gv.vals.get(fldname);
                return v1.equals(v2);
            }
        }
        return false;
    }

    @Override
    public int hashCode(){
        int hashval = 0;
        for (Constant c : vals.values()){
            hashval += c.hashCode();
        }
        return hashval;
    }
}
