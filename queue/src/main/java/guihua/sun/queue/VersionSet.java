package guihua.sun.queue;

import guihua.sun.queue.codec.CodecException;
import guihua.sun.queue.codec.protocol.TBinaryProtocol;
import guihua.sun.queue.codec.protocol.TProtocol;
import guihua.sun.queue.codec.transport.TByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * Created by sunguihua on 2019/7/19.
 */
public class VersionSet {

    private volatile Version current;

    private LinkedList<VersionEdit> edits = new LinkedList<>();

    ByteBuffer buffer = ByteBuffer.allocate(1024);//TODO 未经计算

    private KFile manifestFile;
    private SLog wal;


    public VersionSet(KFile manifestFile) {
        this.manifestFile = manifestFile;
        this.wal = new SLog(manifestFile.getFile().toPath());

        boolean hasNext = true;
        while (hasNext) {

            VersionEdit versionEdit = null;
            try (SLog.Reader reader = wal.reader()) {

                byte[] source = reader.read();
                ByteBuffer byteBuffer = ByteBuffer.wrap(source);
                TByteBuffer tByteBuffer = new TByteBuffer(buffer);
                tByteBuffer.clear();
                TProtocol protocol = new TBinaryProtocol(tByteBuffer);

                versionEdit = new VersionEdit();
                versionEdit.read(protocol);

            } catch (IOException e) {
                throw new IllegalStateException("读取manifest文件异常(1)",e);
            } catch (SLog.EofException e) {
                hasNext = false;
            } catch (CodecException e) {
                throw new IllegalStateException("读取manifest文件异常(2)",e);
            }

            this.append(versionEdit);

        }
    }


    public VersionSet(Version version) {
        this.current = version;
    }

    public synchronized void append(VersionEdit versionEdit) {

        try (SLog.Writer writer = this.wal.writer()) {


            TByteBuffer buffer = new TByteBuffer(this.buffer);
            buffer.clear();
            TProtocol protocol = new TBinaryProtocol(buffer);
            versionEdit.write(protocol);
            writer.write(buffer.toByteArray());
        } catch (IOException | CodecException e) {
            throw new IllegalStateException("manifest文件写入异常", e);
        }

        Version ct = null;
        try {
            ct = (Version) this.current.clone();
        } catch (CloneNotSupportedException e) {
            //ignore
        }

        if (versionEdit.isHasLogNumber()) {

        }

    }

}
