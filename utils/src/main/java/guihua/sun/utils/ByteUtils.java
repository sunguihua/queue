package guihua.sun.utils;

/**
 * Created by sunguihua on 2019/6/11.
 */
public class ByteUtils {

    public static boolean isEqual(byte[] a, byte[] b) {
        assert a != null && b != null;
        if (a.length != b.length) {
            return false;
        }

        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }

        return true;
    }
}
