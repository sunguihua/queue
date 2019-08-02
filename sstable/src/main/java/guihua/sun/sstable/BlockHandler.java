package guihua.sun.sstable;

import guihua.sun.utils.Protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockHandler {
    private long offset;
    private int size;

    public BlockHandler(long offset, int size) {
        this.offset = offset;
        this.size = size;
    }

    public static BlockHandler decodeFixed(ByteBuffer buffer) {
        buffer.rewind();
        long offset = buffer.getLong();
        int size = buffer.getInt();
        return new BlockHandler(offset, size);
    }

    public static BlockHandler decodeVar(ByteBuffer buffer) {
        return null;
    }

    /**
     * 固定长度序列化
     *
     * @param buffer
     */
    public void encodeFixed(ByteBuffer buffer) {
        buffer.putLong(this.offset);
        buffer.putInt(this.size);
    }

    /**
     * 可变长度序列化
     *
     * @return
     */
    public byte[] encodeVar() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            Protocol.writeUnsignedVarLong(this.offset, baos);
            Protocol.writeUnsignedVarInt(this.size, baos);
        } catch (IOException e) {
        }

        return baos.toByteArray();
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }
}