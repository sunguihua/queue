package guihua.sun.queue2;

import guihua.sun.queue.FileMetaData;
import guihua.sun.queue.SLog;
import guihua.sun.queue.VersionEdit;
import guihua.sun.queue.codec.CodecException;
import guihua.sun.queue.codec.protocol.TBinaryProtocol;
import guihua.sun.queue.codec.protocol.TProtocol;
import guihua.sun.queue.codec.transport.TByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public class DefaultStoreMeta implements StoreMeta {

    //-----------内部属性----------------
    private Path manifestFile;

    //------------版本信息---------------
    private long logNo;
    private long preLogNo;
    private long maxSeqNo = 1l;
    private long nextFileNo = 0l;

    private Collection<FileMetaData> files;


    public DefaultStoreMeta(Path manifestFile) {
        this.manifestFile = manifestFile;
    }

    @Override
    public long getMaxSeqNo() {
        return this.maxSeqNo;
    }

    @Override
    public long getNextFileNo() {
        return this.nextFileNo;
    }

    @Override
    public List<FileMetaData> getSuitedTables(Key key) {
        return null;
    }

    public void load() {
        boolean hasNext = true;
        while (hasNext) {
            VersionEdit versionEdit = null;
            try {
                SLog.Reader reader = new SLog(this.manifestFile).reader();
                byte[] source = reader.read();
                ByteBuffer byteBuffer = ByteBuffer.wrap(source);
                TByteBuffer tByteBuffer = new TByteBuffer(byteBuffer);
                tByteBuffer.clear();
                TProtocol protocol = new TBinaryProtocol(tByteBuffer);

                versionEdit = new VersionEdit();
                versionEdit.read(protocol);

            } catch (IOException e) {
                throw new IllegalStateException("读取manifest文件异常(1)", e);
            } catch (SLog.EofException e) {
                hasNext = false;
            } catch (CodecException e) {
                throw new IllegalStateException("读取manifest文件异常(2)", e);
            }

            this.append(versionEdit, false);
        }
    }

    @Override
    public void append(VersionEdit ve, boolean write) {

        if (write) {
            try {
                SLog slog = new SLog(this.manifestFile);
                SLog.Writer writer = slog.writer();

                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                TByteBuffer buffer = new TByteBuffer(byteBuffer);
                buffer.clear();
                TProtocol protocol = new TBinaryProtocol(buffer);
                ve.write(protocol);
                writer.write(buffer.toByteArray());
            } catch (IOException | CodecException e) {
                throw new IllegalStateException("manifest文件写入异常", e);
            }
        }

        if (ve.isHasLastSeqNumber()) {
            this.maxSeqNo = ve.getLastSeqNumber();
        }

        if (ve.isHasNextFileNumber()) {
            this.nextFileNo = ve.getNextFileNumber();
        }

        if (ve.isHasLogNumber()) {
            this.logNo = ve.getLogNumber();
        }

        if (ve.isHasPreLogNumber()) {
            this.preLogNo = ve.getPreLogNumber();
        }

        if (ve.getNewFiles() != null && ve.getNewFiles().size() > 0) {
            //TODO add new files
        }

        if (ve.getDelFiles() != null && ve.getDelFiles().size() > 0) {
            //TODO del files
        }
    }


    @Override
    public long getLogNo() {
        return this.logNo;
    }

    @Override
    public long getPreLogNo() {
        return this.preLogNo;
    }

    public Path getManifestFile() {
        return manifestFile;
    }

    public void setManifestFile(Path manifestFile) throws IOException {
        this.manifestFile = manifestFile;
    }
}
