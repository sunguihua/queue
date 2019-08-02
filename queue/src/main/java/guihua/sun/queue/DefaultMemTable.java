package guihua.sun.queue;

import java.util.*;

import static guihua.sun.queue.StorageKey.*;
import static guihua.sun.utils.Protocol.*;

/**
 * Created by sunguihua on 2019/7/7.
 */
public class DefaultMemTable {


    private long memoryUsed = 0;
    private SortedMap<StorageKey, byte[]> table;

    private InternalComparator internalComparator;


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


    public static class InternalComparator implements Comparator<StorageKey> {

        private Comparator<byte[]> udfComparator;

        @Override
        public int compare(StorageKey o1, StorageKey o2) {

            int foo = udfComparator.compare(o1.getKeyData(), o2.getKeyData());
            return foo != 0 ? foo : (int) (o1.getSeqNo() - o2.getSeqNo());
        }
    }




}
