package query;

/**
 *  Constant needs to be Comparable, so we should override CompareTo()
 */
public class Constant implements Comparable<Constant>{
    private Integer ival = null;
    private String  sval = null;

    public Constant(Integer ival) {
        this.ival = ival;
    }

    public Constant(String sval) {
        this.sval = sval;
    }

    public int asInt() {
        return ival;
    }

    public String asString() {
        return sval;
    }

    @Override
    public boolean equals(Object obj) {
        Constant c = (Constant) obj;
        return (ival != null) ? ival.equals(c.ival) : sval.equals(c.sval);
    }

    @Override
    public int compareTo(Constant c) {
        return (ival != null) ? ival.compareTo(c.ival) : sval.compareTo(c.sval);
    }

    @Override
    public int hashCode() {
        return (ival != null) ? ival.hashCode() : sval.hashCode();
    }

    @Override
    public String toString() {
        return (ival != null) ? ival.toString() : sval.toString();
    }
}


//
///**
// *  Constant needs to be Comparable, so we should override CompareTo()
// */
//public class Constant implements Comparable<Constant> {
//    private Integer numval = null;
//    private String strval = null;
//
//    public Constant(Integer val){
//        this.numval = val;
//    }
//
//    public Constant(String str){
//        this.strval = str;
//    }
//
//    public Integer getNumval() {
//        return numval;
//    }
//
//    public String getStrval() {
//        return strval;
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        Constant constant = (Constant) o;
//        return Objects.equals(numval, constant.numval) ||
//                Objects.equals(strval, constant.strval);
//    }
//
//    @Override
//    public int hashCode() {
//        if(numval != null)
//            return Objects.hash(numval);
//        else
//            return Objects.hashCode(strval);
//    }
//
//    @Override
//    public int compareTo(Constant constant) {
//        if( constant == null || this.getClass() != constant.getClass()) {
//            try {
//                throw new Exception();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        if (this == constant) {
//            return 0;
//        }
//        if (this.numval != null){
//            numval.compareTo(constant.numval);
//        }else{
//            strval.compareTo(constant.strval);
//        }
//
//        return 0;
//    }
//
//    @Override
//    public String toString() {
//        if(numval != null){
//            return numval.toString();
//        }
//        else{
//            return strval;
//        }
//    }
//}
