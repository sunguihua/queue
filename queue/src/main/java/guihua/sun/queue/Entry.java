package guihua.sun.queue;

import guihua.sun.utils.Protocol;

import java.nio.ByteBuffer;

/**
 * Created by sunguihua on 2019/7/7.
 */
public class Entry {
    private long seqNo;
    private byte[] key;
    private byte[] value;

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public ByteBuffer encode(){
        ByteBuffer buffer = ByteBuffer.allocate(key.length + value.length + 19);
        buffer.put(Protocol.writeUnsignedVarInt(key.length));
        buffer.put(key);
        buffer.put(Protocol.writeUnsignedVarLong(seqNo));
        buffer.put(Protocol.writeUnsignedVarInt(value.length));
        buffer.put(value);

        return buffer;
    }
}
