package guihua.sun.sstable;

import guihua.sun.utils.Protocol;
import guihua.sun.utils.Slice;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by sunguihua on 2019/6/3.
 */
public class BlockIndex {

    private List<IndexEntry> recordList = new ArrayList<>();
    private Comparator<Slice> comparator;
    private int maxIndex;

    public BlockIndex(Iterator<Record> recordIterator, Comparator<Slice> comparator) throws IOException {
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();

            Slice value = record.getValue();
            value.rewind();
            this.recordList.add(new IndexEntry(record.getKey(),
                    value.readUnsignedVarLong(),
                    value.readUnsignedVarInt()
            ));
        }

        this.maxIndex = recordList.size() - 1;
        this.comparator = comparator;
    }

    public Iterator<BlockHandler> iterator() {
        Iterator<IndexEntry> indexEntryIterator = this.recordList.iterator();
        return new Iterator<BlockHandler>() {

            @Override
            public boolean hasNext() {
                return indexEntryIterator.hasNext();
            }

            @Override
            public BlockHandler next() {
                IndexEntry entry = indexEntryIterator.next();
                return new BlockHandler(entry.getOffset(), entry.getSize());
            }
        };
    }

    public Optional<BlockHandler> seek(Slice key) {
        IndexEntry max = recordList.get(maxIndex);
        if (this.comparator.compare(key, max.getMaxKey()) > 0) {
            return Optional.empty();
        }

        IndexEntry entry = seek(key, 0, this.maxIndex);

        BlockHandler handler = new BlockHandler(entry.getOffset(), entry.getSize());
        return Optional.of(handler);
    }

    private IndexEntry seek(Slice key, int a, int b) {

        if (a == b) {
            return recordList.get(a);
        }

        assert a <= b;

        int mid = (a + b) / 2;
        IndexEntry rm = recordList.get(mid);

        if (comparator.compare(key, rm.getMaxKey()) <= 0) {
            return seek(key, a, mid);
        } else {
            return seek(key, mid + 1, b);
        }
    }

    private static class IndexEntry {
        Slice maxKey;
        long offset;
        int size;

        public IndexEntry(Slice maxKey, long offset, int size) {
            this.maxKey = maxKey;
            this.offset = offset;
            this.size = size;
        }

        public Slice getMaxKey() {
            return maxKey;
        }

        public long getOffset() {
            return offset;
        }

        public int getSize() {
            return size;
        }
    }
}
