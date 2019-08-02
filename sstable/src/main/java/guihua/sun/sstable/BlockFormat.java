package guihua.sun.sstable;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import guihua.sun.utils.Protocol;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorOutputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static guihua.sun.sstable.Options.COMPRESS_TYPE_NONE;
import static guihua.sun.sstable.Options.COMPRESS_TYPE_SNAPPY_FRAMED;

/**
 * Created by sunguihua on 2019/6/3.
 */
public class BlockFormat {


    private byte[] data;
    private byte compressType;
    private int crc;

    private BlockFormat(byte[] data, byte compressType, int crc) {
        this.data = data;
        this.compressType = compressType;
        this.crc = crc;
    }


    public static BlockFormat decode(byte[] data) {

        int size = data.length;
        byte compressType = data[size - 5];
        int crc32 = Protocol.readFixedInt32(data, size - 4);

        ByteInputStream byteInputStream = new ByteInputStream(data, 0, size - 5);


        InputStream compressorIs = null;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            switch (compressType) {
                case COMPRESS_TYPE_NONE:
                    compressorIs = byteInputStream;
                    break;
                case COMPRESS_TYPE_SNAPPY_FRAMED:
                    compressorIs = new CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.SNAPPY_FRAMED, byteInputStream);
                    break;
                default:
                    throw new IllegalArgumentException("未知的压缩类型");
            }


            byte[] buffer = new byte[1024];

            int length = 0;
            while ((length = compressorIs.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, length);
            }

            byte[] rawData = byteArrayOutputStream.toByteArray();
            int expectedCrc32 = (int) getCrc32(rawData, compressType);

            if (expectedCrc32 != crc32) {
                throw new RuntimeException("block校验码不匹配");
            }

            return new BlockFormat(rawData, compressType, crc32);

        } catch (CompressorException e) {
            throw new RuntimeException("解压异常", e);
        } catch (IOException e) {
            throw new RuntimeException("解压写入异常", e);
        } finally {
            try {
                compressorIs.close();
            } catch (IOException e) {
                //ignore
            }

        }
    }

    public static void encode(byte[] raw, byte compressType, OutputStream os) throws IOException {

        OutputStream compressorOs = null;

        try {
            switch (compressType) {
                case COMPRESS_TYPE_NONE:
                    compressorOs = os;
                    break;
                case COMPRESS_TYPE_SNAPPY_FRAMED:
                    compressorOs = new CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.SNAPPY_FRAMED, os);
                    break;
                default:
                    throw new IllegalArgumentException("未知的压缩类型");

            }

            compressorOs.write(raw);
            compressorOs.flush();
        } catch (CompressorException e) {
            throw new RuntimeException("压缩异常", e);
        } catch (IOException e) {
            throw new RuntimeException("压缩写入异常", e);
        } finally {
            try {
                compressorOs.close();
            } catch (IOException e) {
                //ignore
            }

        }

        try {
            os.write(compressType);
        } catch (IOException e) {
            throw new RuntimeException("写入异常", e);
        }

        int crc32Value = (int) getCrc32(raw, compressType);
        Protocol.writeFixedInt32((int) crc32Value, os);
    }

    private static long getCrc32(byte[] raw, byte compressType) {
        CRC32 crc32 = new CRC32();
        crc32.update(raw);
        crc32.update(compressType);

        return crc32.getValue();
    }

    public byte[] getData() {
        return data;
    }

    public byte getCompressType() {
        return compressType;
    }

    public int getCrc() {
        return crc;
    }
}
