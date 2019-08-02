package guihua.sun.utils;

/**
 * Created by sunguihua on 2019/6/11.
 */
public class IntegerSlice extends Slice {

    public IntegerSlice(int i) {
        super(Protocol.writeUnsignedVarInt(i));
    }
}
