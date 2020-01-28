package guihua.sun.queue2;

import guihua.sun.queue.FileMetaData;
import guihua.sun.queue.SLog;
import guihua.sun.queue.VersionEdit;
import guihua.sun.utils.Protocol;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class Store<K extends Key> {

    private Path home;

    private MemStore curMem;
    private MemStore priMem;

    private Wal curWal;
    private Wal priWal;

    private AtomicLong seqNo;

    private StoreMeta storeMeta;

    private Comparator<InternelKey<K>> internelKeyComparator;


    public Store(Path home, Comparator<K> comparator) {
        this.home = home;
        this.internelKeyComparator = new Comparator<InternelKey<K>>() {
            @Override
            public int compare(InternelKey<K> i1, InternelKey<K> i2) {
                int foo = comparator.compare(i1.getKey(), i2.getKey());
                if (foo != 0) {
                    return foo;
                }
                return i1.getSeqNo() < i2.getSeqNo() ? -1 : 1;
            }
        };
    }

    //--------------------------运行时------------------------
    public void put(K key, byte[] data) {
        long seqNo = getNextSeqNo();
        InternelKey ikey = new InternelKey(seqNo, key);
        InternelValue ival = new InternelValue(1, data);
        Cell msgEdit = getCell(ikey, ival);
        try {
            curWal.append(msgEdit);
        } catch (IOException e) {
            e.printStackTrace();
        }
        curMem.put(ikey, ival);
    }


    public byte[] get(K key) {
        InternelValue ival = curMem.get(key);

        if (ival == null && priMem != null) {
            ival = priMem.get(key);
        }

        if (ival == null) {
            List<FileMetaData> fmdList = getSSTable(key);

            Comparator<InternelKey<K>> comparator = getInternelKeyComparator();
            Cell latest = null;
            for (FileMetaData fmd : fmdList) {
//                Cell cell = ssTable.getCell(key);
//
//                if (comparator.compare(latest.getInternelKey(), cell.getInternelKey()) < 0) {
//                    latest = cell;
//                }
            }

            if (latest != null) {
                ival = latest.getInternelValue();
            }
        }

        if (ival == null) {
            return null;
        }

        if (ival.getAction() == 2) {
            return null;
        }

        return ival.getData();

    }

    private Comparator<InternelKey<K>> getInternelKeyComparator() {
        return this.internelKeyComparator;
    }

    private List<FileMetaData> getSSTable(Key key) {
        //将匹配sstable用StoreMeta代理，便于支持多种文件策略
        return this.storeMeta.getSuitedTables(key);
    }


    private Cell getCell(InternelKey ikey, InternelValue ival) {
        return new Cell(ikey, ival);
    }

    private long getNextSeqNo() {
        return seqNo.getAndIncrement();
    }

    //--------------------------bootstrap------------------------


    public void start() throws IOException {

        Path current = home.resolve(FileNames.CURRENT);

        boolean fromScratch = false;
        if (!Files.exists(current)) {
            //no current file
            fromScratch = true;
            createNew();
            return;
        }

        try {
            byte[] curData = Files.readAllBytes(current);
            if (curData == null || curData.length == 0) {
                fromScratch = true;
                createNew();
                return;
            }

            Path manifestFile = home.resolve(new String(curData));
            this.storeMeta = resolveManifest(manifestFile);

            long maxSeqNo = storeMeta.getMaxSeqNo();
            this.seqNo = new AtomicLong(maxSeqNo);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private StoreMeta createNew() throws IOException {

        Path manifestFile = FileNames.getManifestFile(this.home, 0, true);
        updateCurrent(manifestFile);
        StoreMeta storeMeta = new DefaultStoreMeta(manifestFile);
        long walFileNo = storeMeta.getNextFileNo();

        Path walFile = FileNames.getWal(home, walFileNo, true);

        VersionEdit newLogEdit = new VersionEdit();
        newLogEdit.setLogNumber(walFileNo);

        storeMeta.append(newLogEdit, true);

        this.curWal = resolveWal(walFile);
        this.curMem = createMemStore();

        this.seqNo = new AtomicLong(storeMeta.getMaxSeqNo());
        return storeMeta;
    }

    private void updateCurrent(Path manifest) throws IOException {

        FileAttribute<Set<PosixFilePermission>> attr =
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path temp = Files.createTempFile(FileNames.CURRENT, null, attr);

        Files.write(temp, manifest.getFileName().toString().getBytes());

        Files.move(temp, FileNames.current(this.home), REPLACE_EXISTING);
    }

    public StoreMeta resolveManifest(Path manifestPath) throws IOException {

        // 1读取manifest文件
        DefaultStoreMeta storeMeta = new DefaultStoreMeta(manifestPath);
        storeMeta.load();

        // 2将wal文件读取到内存中。

        Path priWalFile = FileNames.getWal(this.home, storeMeta.getPreLogNo(), false);

        if (priWalFile != null && Files.exists(priWalFile)) {
            this.priMem = createMemStore();
            this.priWal = new DefaultWal(priWalFile);
            Iterator<Cell> cellIterator = priWal.iterator();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                priMem.put(cell.getInternelKey(), cell.getInternelValue());
            }
        }

        Path curWalFile = FileNames.getWal(this.home, storeMeta.getLogNo(), false);
        this.curMem = createMemStore();
        if (curWalFile != null && Files.exists(curWalFile)) {
            this.curWal = new DefaultWal(curWalFile);
            Iterator<Cell> cellIterator = curWal.iterator();
            while (cellIterator.hasNext()) {
                Cell cell = cellIterator.next();
                curMem.put(cell.getInternelKey(), cell.getInternelValue());
            }
        }

        //3 清理掉没有引用的文件

        //TODO P2

        return storeMeta;
    }

    private Wal resolveWal(Path walFile) throws IOException {
        return new DefaultWal(walFile);
    }

    private MemStore createMemStore() {
        return new DefaultMemTable(this.internelKeyComparator);
    }


}
