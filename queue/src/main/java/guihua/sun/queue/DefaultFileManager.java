package guihua.sun.queue;

import guihua.sun.utils.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultFileManager implements FileManager {


    private final static Logger LOG = LoggerFactory.getLogger(DefaultFileManager.class);

    private final static long _currentFileNumber = -1;

    private File home;

    private AtomicLong nextFileNumber;

    private Map<Long, KFile> metas = new ConcurrentHashMap<>();

    public DefaultFileManager(String homePath) {
        File home = new File(homePath);
        if (!home.isDirectory()) {
            throw new IllegalArgumentException("home目录不是文件夹");
        }
        this.home = home;

        for (File file : home.listFiles()) {
            String fileName = file.getName();
            if (fileName.equals("current")) {
                KFile kFile = new DefaultKFile(_currentFileNumber, file, KFileType.Current, new AtomicLong(0));

                if (metas.put(_currentFileNumber, kFile) != null) {
                    throw new IllegalStateException("存在多个current文件");
                }
            } else if (fileName.endsWith(".mf")) {

                long fileNumber = Long.valueOf(fileName.substring(0, fileName.length() - 3));

                KFile kFile = new DefaultKFile(fileNumber, file, KFileType.Manifest, new AtomicLong(0));
                if (metas.put(fileNumber, kFile) != null) {
                    throw new IllegalStateException("已有文件号为\" + fileNumber + \"的文件！（1）");
                }
            } else if (fileName.endsWith(".log")) {
                long fileNumber = Long.valueOf(fileName.substring(0, fileName.length() - 4));

                KFile kFile = new DefaultKFile(fileNumber, file, KFileType.Log, new AtomicLong(0));
                if (metas.put(fileNumber, kFile) != null) {
                    throw new IllegalStateException("已有文件号为\" + fileNumber + \"的文件！（2）");
                }
            } else if (fileName.endsWith(".sst")) {
                long fileNumber = Long.valueOf(fileName.substring(0, fileName.length() - 4));

                KFile kFile = new DefaultKFile(fileNumber, file, KFileType.SSTable, new AtomicLong(0));
                if (metas.put(fileNumber, kFile) != null) {
                    throw new IllegalStateException("已有文件号为\" + fileNumber + \"的文件！（3）");
                }
            } else {
                LOG.warn("未知的文件类型.name:{}", fileName);
            }
        }

    }


    @Override
    public KFile create(KFileType fileType) {
        //TODO 创建kfile
        return null;
    }

    @Override
    public KFile get(long fileNumber) {

        return null;
    }

    @Override
    public KFile getManifest() {

        KFile current = metas.get(_currentFileNumber);
        if (current == null) {
            return null;
        }
        long fileNumber = readManifestFileNo(current.getFile());
        KFile ret = metas.get(fileNumber);
        if (ret.getType() != KFileType.Manifest) {
            throw new IllegalStateException("Current文件保存Manifest文件号异常,类型不匹配");
        }
        return ret;
    }

    private long readManifestFileNo(File file) {

        try (InputStream is = new FileInputStream(file)) {

            if (is.available() < 2) {
                throw new IllegalStateException("current文件内容异常:小于2个字节");
            }
            return Protocol.readUnsignedVarLong(is);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("current文件不存在", e);
        } catch (IOException e) {
            throw new IllegalStateException("读取current文件异常", e);
        }
    }


    @Override
    public void compactManifest() {

    }

    @Override
    public void recycle() {
        //TODO 在初始化VersionSet后，回收未被引用的文件
    }

    void recycle(long fileNumber) {
        KFile kFile = metas.remove(fileNumber);
        File file = kFile.getFile();
        if (!file.delete()) {
            LOG.warn("删除文件失败.文件名:{}", file.getName());
        }
    }

    class DefaultKFile implements KFile {

        public DefaultKFile(long fileNumber, File file, KFileType fileType, AtomicLong refCount) {
            this.refCount = refCount;
            this.file = file;
            this.fileType = fileType;
            this.fileNumber = fileNumber;
        }

        private AtomicLong refCount;
        private File file;
        private KFileType fileType;
        private long fileNumber;

        @Override
        public long getNumber() {
            return this.fileNumber;
        }

        @Override
        public KFileType getType() {
            return this.fileType;
        }

        @Override
        public void ref() {
            this.refCount.incrementAndGet();
        }

        @Override
        public void unref() {
            if (this.refCount.decrementAndGet() <= 0) {
                recycle(this.fileNumber);
            }
        }

        @Override
        public File getFile() {
            return this.file;
        }

        @Override
        public void mark() {

        }

        @Override
        public boolean isLive() {
            return false;
        }
    }
}
