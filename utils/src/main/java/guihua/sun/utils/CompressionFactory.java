package guihua.sun.utils;

/**
 * Created by sunguihua on 2019/5/30.
 */
public interface CompressionFactory {

    public Compressor getCompressor(byte type);

    public Decompressor getDecompressor(byte type);

}
