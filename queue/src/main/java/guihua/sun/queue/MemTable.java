package guihua.sun.queue;

/**
 * Created by sunguihua on 2019/8/2.
 */
public interface MemTable {

    long seqNo();

    void recover(KFile kFile);

    boolean immutable();

    void immute();

    void put(byte[] userKey, byte[] userValue);

    InternalKey smallestKey();

    InternalKey largestKey();

    long estimateNeededStoreSize();
}
