package guihua.sun.queue;

import guihua.sun.queue.event.CompactEventListener;
import guihua.sun.queue.event.CompactionEvent;
import guihua.sun.queue.event.MajorCompactEvent;
import guihua.sun.queue.event.MinorCompactEvent;

import java.io.IOException;
import java.util.*;

/**
 * Created by sunguihua on 2019/7/7.
 */
public class DefaultPointer implements Pointer, CompactEventListener {


    private int levelCount;
    private DefaultMemTable memTable;
    private MemTableReader memTableReader;

    private LevelReader[] levelReaders;
    private LinkedList<Entry>[] levelCaches;

    private TreeMap<SSTableReader, LinkedList<Entry>> readerAndCache = new TreeMap<>();

    private Set<MajorCompactEvent> majorCompactEvents = Collections.synchronizedSet(new LinkedHashSet<>());

    private Comparator<Entry> comparator;

    private Entry next;
    private Entry last;


    @Override
    public synchronized Entry pull() {



        return null;
    }


    private Optional<Entry> get(SSTableReader reader, final LinkedList<Entry> cache) {

        Entry entry = cache.peek();
        if (peek() != null) {
            return Optional.of(entry);
        }
        MajorCompactEvent affectedEven = searchCompactionEvent(reader);

        if (affectedEven != null) {
            adjustMajorCompaction(affectedEven);
            return Optional.empty();
        } else {
            Boolean isEOF = reader.readBlock(cache);
            if (isEOF) {
                remove(reader);
            }

            if (cache.size() == 0) {
                throw new IllegalStateException("读取下一个block返回列表为空");
            }

            return Optional.of(cache.peek());
        }
    }


    private void remove(SSTableReader reader) {

    }




    @Override
    public Entry peek() {
        LinkedList<Entry> pickedCache = null;
        Entry picked = this.memTableReader.peek();

        for (Map.Entry<SSTableReader, LinkedList<Entry>> readerAndCache : this.readerAndCache.entrySet()) {
            SSTableReader reader = readerAndCache.getKey();
            LinkedList<Entry> cache = readerAndCache.getValue();

            Optional<Entry> candidate = get(reader, cache);

            if(!candidate.isPresent()){
                continue;
            }

            if (this.comparator.compare(picked, candidate.get()) > 0){
                picked = candidate.get();
                pickedCache = cache;  //也就是说如果从sstable读取，必然先加载到cache中
            }
        }
        return picked;
    }

    @Override
    public void commit(long seqNo) {

    }

    @Override
    public void close() throws IOException {

    }


    @Override
    public void onCompact(CompactionEvent event) {

        if (event instanceof MajorCompactEvent) {
            this.majorCompactEvents.add((MajorCompactEvent) event);
        }

        adjustMinCompaction((MinorCompactEvent) event);
    }

    private MajorCompactEvent searchCompactionEvent(SSTableReader reader) {

        SSTable ssTable = reader.getSSTable();
        for (MajorCompactEvent event : this.majorCompactEvents) {
            List<SSTable> delTables = event.getDelSSTables();
            if (delTables.contains(ssTable)) {
                this.majorCompactEvents.remove(event);
                return event;
            }
        }
        return null;
    }

    private void adjustMajorCompaction(MajorCompactEvent event) {

        //删除所有delTables的Reader和Cache
        //增加所有addTables对应的Reader和Cache

    }

    private synchronized void adjustMinCompaction(MinorCompactEvent minorCompactEvent) {
        this.memTable = minorCompactEvent.getNewMemTable();
        this.memTableReader = memTable.getReader(last.getKey(), last.getSeqNo());

        SSTableReader ssTableReader = minorCompactEvent.getMewSStable().getReader(last.getKey(), last.getSeqNo());
        LinkedList<Entry> cache = new LinkedList<>();

        this.readerAndCache.put(ssTableReader, cache);
    }
}
