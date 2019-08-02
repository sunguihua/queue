package guihua.sun.queue;

/**
 * Created by sunguihua on 2019/7/29.
 */
public class StorageKey {

    private long seqNo;
    private byte[] keyData;

    public static StorageKey smallest(byte[] keyData) {
        return new StorageKey(0, keyData);
    }

    public static StorageKey biggest(byte[] keyData) {
        return new StorageKey(Long.MAX_VALUE, keyData);
    }

    public static StorageKey storageKey(long seqNo, byte[] keyData) {
        assert seqNo > 0;
        return new StorageKey(seqNo, keyData);
    }

    public StorageKey(long seqNo, byte[] keyData) {
        this.seqNo = seqNo;
        this.keyData = keyData;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public byte[] getKeyData() {
        return keyData;
    }

    public void setKeyData(byte[] keyData) {
        this.keyData = keyData;
    }
}
