package guihua.sun.sstable;

import guihua.sun.utils.CompressionFactory;
import guihua.sun.utils.Compressor;
import guihua.sun.utils.Protocol;
import io.netty.buffer.ByteBuf;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;

import static io.netty.buffer.Unpooled.*;

/**
 * Created by sunguihua on 2019/5/29.
 */
public class BlockBuilder {

    private int blockSize;
    private int restartInterval;
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream(blockSize * 2);
    private List<Integer> rpOffsets = new LinkedList<>();
    private int count;
    private byte[] lastKey;

    public BlockBuilder(int blockSize, int restartInterval) {
        this.blockSize = blockSize;
        this.restartInterval = restartInterval;
        this.count = this.restartInterval;
    }

    //添加到当前buffer
    void append(byte[] key, byte[] value) throws IOException {

        if (count++ >= restartInterval) {

            rpOffsets.add(buffer.size());
            count = 1;

            Protocol.writeUnsignedVarInt(0, buffer);
            Protocol.writeUnsignedVarInt(key.length, buffer);
            Protocol.writeUnsignedVarInt(value.length, buffer);
            buffer.write(key);
            buffer.write(value);
        } else {
            int sharedLength = overlapping(this.lastKey, key);
            int unsharedLength = key.length - sharedLength;

            Protocol.writeUnsignedVarInt(sharedLength, buffer);
            Protocol.writeUnsignedVarInt(unsharedLength, buffer);
            Protocol.writeUnsignedVarInt(value.length, buffer);

            buffer.write(key, sharedLength, unsharedLength);
            buffer.write(value);

        }
        this.lastKey = key;
    }

    private int overlapping(byte[] rpKey, byte[] cpKey) {
        int i = 0;
        for (; i < rpKey.length && i < cpKey.length; i++) {
            if (rpKey[i] != cpKey[i]) {
                break;
            }
        }
        return i;
    }

    //判断是否已经满足一个block
    boolean isFull() {
        return buffer.size() >= this.blockSize;
    }

    boolean isEmpty() {
        return buffer.size() == 0;
    }

    //依次将restart-point的偏移量和restart-point的个数写入buffer
    byte[] finish() throws IOException {
        for (Integer offset : rpOffsets) {
            Protocol.writeFixedInt32(offset, buffer);
        }
        Protocol.writeFixedInt32(rpOffsets.size(),buffer);
        return buffer.toByteArray();
    }

    public void reset() {
        this.buffer.reset();
        this.count = this.restartInterval;
        this.lastKey = null;
        this.rpOffsets.clear();
    }
}
