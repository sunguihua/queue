package guihua.sun.sstable;

import guihua.sun.utils.CompressionFactory;
import guihua.sun.utils.Slice;

import java.nio.charset.Charset;
import java.util.Comparator;

/**
 * Created by sunguihua on 2019/5/30.
 */
public class Options {


    public final static byte COMPRESS_TYPE_NONE = (byte) 0;
    public final static byte COMPRESS_TYPE_SNAPPY_FRAMED = (byte) 1;


    private final static int _default_block_size = 4096;
    private final static int _default_restart_point_interval = 4;
    private final static byte _default_compress_type = COMPRESS_TYPE_SNAPPY_FRAMED;

    public static int BLOCK_SIZE = _default_block_size;

    public static int BLOCK_RESTART_POINT_INTERVAL = _default_restart_point_interval;

    public static byte BLOCK_COMPRESS_TYPE = _default_compress_type;

    public static Comparator<Slice> COMPARATOR = new _defaultComparator();


    private static class _defaultComparator implements Comparator<Slice> {
        public int compare(Slice left, Slice right) {

            for (int i = left.getOffset(), j = right.getOffset(); i < left.getLimit() && j < right.getLimit(); i++, j++) {
                int a = (left.getData()[i] & 0xff);
                int b = (right.getData()[j] & 0xff);
                if (a != b) {
                    return a - b;
                }
            }
            return left.getLength() - right.getLength();
        }
    }
}
