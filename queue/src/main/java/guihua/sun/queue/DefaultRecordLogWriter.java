package guihua.sun.queue;

import guihua.sun.utils.Slice;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Created by sunguihua on 2019/6/27.
 */
public class DefaultRecordLogWriter implements RecordLogWriter {

    private final static byte FULL = 0;
    private final static byte FIRST = 1;
    private final static byte MIDDLE = 2;
    private final static byte LAST = 3;

    private final static byte[] occupiedBytes = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};


    private Path path;
    private FileOutputStream fos;

    private int blockSize;
    private int currentBlockOffset = 0;
    private int blockLeftSize = blockSize;

    public DefaultRecordLogWriter(String filePath) throws IOException {
        this.path = path;

        File file = path.toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        this.fos = new FileOutputStream(file);

    }

    public void write(Slice data) {
    }


    public void _write(Slice data, int offset, int loop) throws IOException {
        byte type = 0;
        int recordLength = 0;
        int occupiedSize = 0;
        int dataLeftBytes = data.getLimit() - data.getOffset();

        if (dataLeftBytes <= this.blockLeftSize - 7) {
            recordLength = dataLeftBytes;
            if (loop == 0) {
                type = FULL;
            } else {
                type = LAST;
            }


        } else {
            recordLength = blockLeftSize;
            if (loop == 0) {
                type = FIRST;
            } else {
                type = MIDDLE;
            }
        }

        _physicalWrite(type, data, offset, recordLength);
        this.currentBlockOffset = this.currentBlockOffset + 7 + recordLength;
        this.blockLeftSize = this.blockSize - this.currentBlockOffset;

        if (this.blockLeftSize < 7 && this.blockLeftSize > 0) {
            ByteBuffer buffer = ByteBuffer.allocate(this.blockLeftSize);
            buffer.put(occupiedBytes, 0, this.blockLeftSize);
            //填满block最后occupiedSize个字节
            this.fos.write(this.occupiedBytes, 0, occupiedSize);
            this.currentBlockOffset = 0;
            this.blockLeftSize = blockSize;
        } else if (this.blockLeftSize == 0) {
            this.currentBlockOffset = 0;
            this.blockLeftSize = blockSize;
        }

    }

    private void _physicalWrite(byte type, Slice data, int offset, int length) throws IOException {
        int crc32 = crc32(type, data, offset, length);
        ByteBuffer buffer = ByteBuffer.allocate(7 + length);

        //write crc32 .Little endian
        this.fos.write(crc32);
        this.fos.write(crc32 >> 8);
        this.fos.write(crc32 >> 16);
        this.fos.write(crc32 >> 24);
        this.fos.write(length);
        this.fos.write(length >> 8);
        this.fos.write(type);
        this.fos.write(data.getData(), offset, length);
        this.fos.flush();
    }


    private int crc32(byte type, Slice data, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(type);
        crc32.update(data.getData(), offset, length);
        return (int) crc32.getValue();
    }

    @Override
    public void close() throws IOException {
        this.fos.close();
    }
}
