package buffer;

/**
 * 一个用来给Scan分配最优估算buffers个数的类
 */
public class BufferNeeds {

    /**
     * 这个方法考虑不同roots指定输出的大小
     * 并且返回满足空闲buffer的最高roots
     */
    public static int bestRoot(int available, int size){
        int avail = available - 2;
        if(avail <= 1){
            return 1;
        }
        int k = Integer.MAX_VALUE;
        double i = 1.0;
        while (k > avail) {
            i++;
            k = (int)Math.ceil(Math.pow(size, 1/i));
        }
        return k;
    }

    /**
     * 返回不同factors的指定输出大小
     * 返回满足空闲buffer数的最高factor
     */
    public static int bestFactor(int available, int size){
        int avail = available - 2;  //reserve a couple
        if (avail <= 1) {
            return 1;
        }
        int k = size;
        double i = 1.0;
        while (k > avail) {
            i++;
            k = (int)Math.ceil(size / i);
        }
        return k;
    }
}
