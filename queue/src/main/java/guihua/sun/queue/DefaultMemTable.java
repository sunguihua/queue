package guihua.sun.queue;

import java.util.*;

import static guihua.sun.queue.StorageKey.*;
import static guihua.sun.utils.Protocol.*;

/**
 * Created by sunguihua on 2019/7/7.
 */
public class DefaultMemTable implements MemTable{


    private long memoryUsed = 0;
    private SortedMap<StorageKey, byte[]> table;

    private KFile wal;
    private InternalComparator internalComparator;


    public DefaultMemTable(KFile wal) {
        this.wal = wal;
    }

    public void put(long seqNo, byte[] userKey, byte[] userValue) {

        int entryByteCount = varIntLength(userKey.length) + userKey.length
                + varIntLength(userValue.length) + userValue.length;

        StorageKey storageKey = new StorageKey(seqNo, userKey);
        table.put(storageKey, userValue);
        memoryUsed += entryByteCount;
    }

    public Set<Map.Entry<StorageKey, byte[]>> seek(byte[] key) {
        SortedMap<StorageKey, byte[]> subMap = table.subMap(smallest(key), biggest(key));
        return subMap.entrySet();
    }


    public Iterator<Map.Entry<StorageKey, byte[]>> iterator(long seqNo, byte[] key) {
        assert seqNo > 0;
        assert key != null;
        return table.tailMap(new StorageKey(seqNo, key)).entrySet().iterator();
    }

    public Iterator<Map.Entry<StorageKey, byte[]>> iterator() {
        return this.table.entrySet().iterator();
    }


    long estimateNeededStorageSize() {
        return this.memoryUsed;
    }

    @Override
    public long seqNo() {
        return 0;
    }

    @Override
    public void recover(KFile kFile) {

    }

    @Override
    public boolean immutable() {
        return false;
    }

    @Override
    public void immute() {

    }

    @Override
    public void put(byte[] userKey, byte[] userValue) {

    }

    @Override
    public InternalKey smallestKey() {
        return null;
    }

    @Override
    public InternalKey largestKey() {
        return null;
    }

    @Override
    public long estimateNeededStoreSize() {
        return 0;
    }


    public static class InternalComparator implements Comparator<StorageKey> {

        private Comparator<byte[]> udfComparator;

        @Override
        public int compare(StorageKey o1, StorageKey o2) {

            int foo = udfComparator.compare(o1.getKeyData(), o2.getKeyData());
            return foo != 0 ? foo : (int) (o1.getSeqNo() - o2.getSeqNo());
        }
    }




}
