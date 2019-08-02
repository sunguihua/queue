package guihua.sun.utils;

/**
 * Created by sunguihua on 2019/5/30.
 */
public interface Compressor {

    public byte[] compress(byte[] bytes);

    public byte[] compress(byte[] bytes, int a, int b);
}
