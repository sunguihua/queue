package guihua.sun.sstable;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Created by sunguihua on 2019/5/30.
 */
public class BlockEntry {

    private final int sharedKeyLength;
    private final int unsharedKeyLength;
    private final int valueLength;
    private final byte[] unsharedKey;
    private final byte[] value;

    public BlockEntry(int sharedKeyLength, int unsharedKeyLength, int valueLength, byte[] unsharedKey, byte[] value) {
        this.sharedKeyLength = sharedKeyLength;
        this.unsharedKeyLength = unsharedKeyLength;
        this.valueLength = valueLength;
        this.unsharedKey = unsharedKey;
        this.value = value;
    }

    public int getSharedKeyLength() {
        return sharedKeyLength;
    }

    public int getUnsharedKeyLength() {
        return unsharedKeyLength;
    }

    public int getValueLength() {
        return valueLength;
    }

    public byte[] getUnsharedKey() {
        return unsharedKey;
    }

    public byte[] getValue() {
        return value;
    }

    public void write(ByteBuf blockBuffer) {
        blockBuffer.writeInt(this.sharedKeyLength);
        blockBuffer.writeInt(this.unsharedKeyLength);
        blockBuffer.writeInt(this.valueLength);
        blockBuffer.writeBytes(this.unsharedKey);
        blockBuffer.writeBytes(this.value);
    }
}
