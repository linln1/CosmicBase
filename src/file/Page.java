package file;

import java.nio.ByteBuffer;
import java.nio.charset.*;

public class Page {
    private ByteBuffer bb;
    private Byte[] blob;
    private int blobLen;
    public static Charset CHARSET = StandardCharsets.US_ASCII;

    // The size of an integer in bytes, the value is almost certainly 4, but it's a good idea to encode this as a constant
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    /**
     * The maximum size, in bytes, of a string of length n.
     * A string is represented as the encoding of its characters,
     * preceded by an integer denoting the number of bytes in this encoding.
     * If the JVM uses the US-ASCII encoding, then each char is stored in one byte.
     * So a String of n characters has a size of 4+n bytes.
     * @param STR_SIZE the size of String
     * @return the maximum size of bytes required to store a string of size n
     */
    public static final int StrLEN(int STR_SIZE){
        float bytePerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        return INT_SIZE + (STR_SIZE * (int)bytePerChar);
    }

    // For creating data buffers, The number of bytes in a block, a realistic value would be 4K
    public Page(int blocksize) {
        bb = ByteBuffer.allocateDirect(blocksize);
    }

    // For creating log pages
    public Page(byte[] b) {
        bb = ByteBuffer.wrap(b);
    }

    public synchronized int getInt(int offset) {
        return bb.getInt(offset);
    }

    public synchronized void setInt(int offset, int n) {
        if(offset + n >= bb.capacity()){
            System.out.println("The integer " + n + " does not fit at location " + offset + " of the page.");
        }
        else
            bb.putInt(offset, n);
    }

    public synchronized byte[] getBytes(int offset) {
        bb.position(offset);
        int length = bb.getInt();
        byte[] b = new byte[length];
        bb.get(b);
        return b;
    }

    public synchronized void setBytes(int offset, byte[] b) {
        if(offset + b.length > bb.capacity()){
            System.out.println("The Bytes " + b.length + " does not fit at location " + offset + " of the page.");
            return ;
        }
        //设置下一部分数据的写入位置
        bb.position(offset);
        bb.putInt(b.length);
        bb.put(b);
    }

    public synchronized String getString(int offset) {
        byte[] b = getBytes(offset);
        return new String(b, CHARSET);
    }

    public synchronized void setString(int offset, String s) {
        if(offset + s.length() > bb.capacity()){
            System.out.println("The String " + s + " does not fit at location " + offset + " of the page.");
            return ;
        }
        byte[] b = s.getBytes(CHARSET);
        setBytes(offset, b);
    }

    public static int maxLength(int strlen) {
        float bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
        return Integer.BYTES + (strlen * (int)bytesPerChar);
    }

    // a package private method, needed by FileMgr
    ByteBuffer contents() {
        bb.position(0);
        return bb;
    }
}
