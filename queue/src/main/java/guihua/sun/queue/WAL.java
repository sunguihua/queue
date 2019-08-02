package guihua.sun.queue;

import guihua.sun.utils.Tuple;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Created by sunguihua on 2019/7/24.
 */
public class WAL {


    private final static byte ZERO_TYPE = 0;
    private final static byte FULL_TYPE = 1;
    private final static byte FIRST_TYPE = 2;
    private final static byte MIDDLE_TYPE = 3;
    private final static byte LAST_TYPE = 4;

    private final static int _BLOCK_SIZE = 512;//32kb
    private final static int _BLOCK_HEADER_LENGTH = 7;

    private final static byte[] _tailFiller = {0, 0, 0, 0, 0, 0};


    private Path path;

    public WAL(Path path) {
        this.path = path;
    }

    public Writer writer() throws IOException {
        return new Writer(path);
    }

    public Reader reader() throws IOException {
        return new Reader(_BLOCK_SIZE, path);
    }

    public class Writer implements Closeable {

        private int blockOffset;

        private SeekableByteChannel fileChannel;

        public Writer(Path logFilePath) throws IOException {
            this.fileChannel = Files.newByteChannel(logFilePath, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        }

        public void write(byte[] bytes) throws IOException {

            int offset = 0;
            boolean begin = true;

            while (offset < bytes.length) {

                int leftover = _BLOCK_SIZE - this.blockOffset;
                if (leftover < _BLOCK_HEADER_LENGTH) {
                    if (leftover > 0) {
                        ByteBuffer tailFillerBuffer = ByteBuffer.wrap(_tailFiller, 0, leftover);
                        fileChannel.write(tailFillerBuffer);
                    }
                    blockOffset = 0;
                }

                int avail = _BLOCK_SIZE - this.blockOffset - _BLOCK_HEADER_LENGTH;
                int left = bytes.length - offset;

                int fragmentSize = (avail >= left) ? left : avail;
                boolean end = (fragmentSize == left);

                byte type = ZERO_TYPE;
                if (begin && end) {
                    type = FULL_TYPE;
                } else if (begin) {
                    type = FIRST_TYPE;
                } else if (end) {
                    type = LAST_TYPE;
                } else {
                    type = MIDDLE_TYPE;
                }

                writePhysically(type, ByteBuffer.wrap(bytes, offset, fragmentSize));
                offset += fragmentSize;
                begin = false;
            }

        }


        public void close() throws IOException {
            this.fileChannel.close();
        }


        private void writePhysically(byte type, ByteBuffer buf) throws IOException {

            int size = buf.remaining();
            assert size < 0xffff : "单fragment长度必须小于2个byte的长度";

            CRC32 crc32 = new CRC32();
            crc32.update(type);
            crc32.update(buf);
            int checksum = (int) crc32.getValue();

            ByteBuffer header = ByteBuffer.allocate(_BLOCK_HEADER_LENGTH);
            header.putInt(checksum);
            //长度2个byte，little endian
            header.putChar((char) size);
            header.put(type);

            header.flip();
            fileChannel.write(header);

            buf.flip();
            fileChannel.write(buf);
            blockOffset += _BLOCK_HEADER_LENGTH + size;
        }
    }

    public class Reader implements Closeable {

        private final int blockSize;
        private final ByteBuffer buf;
        private SeekableByteChannel fileChannel;

        private boolean checksum = true;


        private final static byte _EOF = 5;
        private final static byte _ERROR_RECORD = 6;

        private boolean eof = false;

        public Reader(int blockSize, Path path) throws IOException {
            this.blockSize = blockSize;
            this.buf = ByteBuffer.allocate(blockSize);
            this.buf.flip();
            this.fileChannel = Files.newByteChannel(path, StandardOpenOption.READ);
        }

        public byte[] read() throws IOException, EofException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            WritableByteChannel wc = Channels.newChannel(baos);

            boolean end = false;
            boolean inFragmentRecord = false;
            while (!end) {

                Tuple<Byte, ByteBuffer> tuple = readPhysicalRecord();
                switch (tuple._1()) {

                    case FULL_TYPE:
                        if (inFragmentRecord) {
                            report(baos.size(), "没有结尾的残缺Fragment (1)");
                            baos.reset();
                        }
                        wc.write(tuple._2());
                        end = true;
                        break;
                    case FIRST_TYPE:
                        if (inFragmentRecord) {
                            report(baos.size(), "没有结尾的残缺Fragment (2)");
                            baos.reset();
                        }
                        wc.write(tuple._2());
                        inFragmentRecord = true;
                        break;
                    case MIDDLE_TYPE:

                        if (!inFragmentRecord) {
                            report(tuple._2().remaining(), "没有开头的残缺Fragment (1)");
                            break;
                        }

                        wc.write(tuple._2());
                        break;
                    case LAST_TYPE:
                        if (!inFragmentRecord) {
                            report(tuple._2().remaining(), "没有开头的残缺Fragment (2)");
                            break;
                        }
                        wc.write(tuple._2());
                        inFragmentRecord = false;
                        end = true;
                        break;
                    case _EOF:

                        if (inFragmentRecord) {
                            report(baos.size(), "文件结尾，未完结的Fragment");
                        }
                        throw new EofException();

                    case _ERROR_RECORD:

                        if (inFragmentRecord) {
                            report(baos.size(), "中间Fragment错误");
                            inFragmentRecord = false;
                            baos.reset();
                        }
                        break;

                    default:
                        report(baos.size() + tuple._2().remaining(), "未知的Fragment类型");
                        inFragmentRecord = false;
                        baos.reset();
                        break;
                }
            }

            return baos.toByteArray();
        }


        private Tuple<Byte, ByteBuffer> readPhysicalRecord() throws IOException {

            if (buf.remaining() < _BLOCK_HEADER_LENGTH) {
                buf.clear();

                if (eof) {
                    return new Tuple<>(_EOF, null);
                }

                int readLength = fileChannel.read(buf);
                buf.flip();
                eof = readLength < blockSize;
            }


            int mark = buf.getInt();
            char fragmentLength = buf.getChar();

            byte type = buf.get();

            ByteBuffer data;
            data = buf.slice();
            data.mark();

            int position = buf.position();
            if (fragmentLength > buf.remaining()) {
                if (!eof) {
                    report(buf.remaining(),"错误的Fragment长度");
                    buf.position(buf.limit());
                    return new Tuple<>(_ERROR_RECORD, null);
                }
                return new Tuple<>(_EOF, null);
            }
            data.limit(fragmentLength);
            buf.position(position + fragmentLength);

            if (checksum) {
                CRC32 crc32 = new CRC32();
                crc32.update(type);
                crc32.update(data);
                int expectedMark = (int) crc32.getValue();
                data.flip();

                if (expectedMark != mark) {

                    int dropSize = buf.remaining();
                    buf.clear();
                    //report drop
                    return new Tuple<>(_ERROR_RECORD, null);
                }
            }

            return new Tuple<>(type, data);
        }

        @Override
        public void close() throws IOException {
            this.fileChannel.close();
        }
    }

    private void report(int size, String errorMsg) {
        System.out.println(size + " 字节被丢弃，原因:" + errorMsg);
    }


    public class EofException extends Exception {
    }


    public static void main(String[] args) {
        String dir = "/Users/sunguihua/temp";
        Path logPath = Paths.get(dir, "wal.log");
        WAL wal = new WAL(logPath);
//        try (Writer writer = wal.writer()) {
//
//            for(int j=0;j<1000;j++){
//                int random = ThreadLocalRandom.current().nextInt(256);
//                System.out.println("=========" + random + "=============");
//                StringBuffer sb = new StringBuffer();
//                for (int i = 0; i < random; i++) {
//                    sb.append(random);
//                }
//
//                writer.write(sb.toString().getBytes());
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        try (Reader reader = wal.reader()) {

            while (true) {
                try {
                    byte[] bytes = reader.read();
                    System.out.println("===" + new String(bytes));
                } catch (EofException e) {
                    System.out.println("===end of file ===");
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
