package guihua.sun.sstable;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static guihua.sun.sstable.Options.*;
import static java.nio.file.StandardOpenOption.*;

/**
 * Created by sunguihua on 2019/5/31.
 */
public class SSTableBuilder implements Closeable {

    private Path path;
    private FileChannel fc;

    private BlockBuilder dataBlockBuilder;

    private BlockBuilder indexBlockBuilder;

    public SSTableBuilder(Path path) throws IOException {
        this.path = path;

        File file = path.toFile();
        if (!file.exists()) {
            file.createNewFile();
        }

        this.fc = FileChannel.open(path, WRITE);

        dataBlockBuilder = new BlockBuilder(BLOCK_SIZE, BLOCK_RESTART_POINT_INTERVAL);
        indexBlockBuilder = new BlockBuilder(BLOCK_SIZE, BLOCK_RESTART_POINT_INTERVAL);
    }


    private byte[] blockMinKey;
    private byte[] lastKey;
    private long curBlockOffset = 0;

    public void append(byte[] key, byte[] value) throws IOException {

        dataBlockBuilder.append(key, value);
        this.lastKey = key;
        if (dataBlockBuilder.isFull()) {
            BlockHandler dataBlockHandler = flush(dataBlockBuilder);
            indexBlockBuilder.append(key, dataBlockHandler.encodeVar());
            this.curBlockOffset = fc.position();
        }

    }


    public void finish() throws IOException {
        BlockHandler dataBlockHandler = flush(dataBlockBuilder);

        indexBlockBuilder.append(lastKey, dataBlockHandler.encodeVar());

        BlockHandler indexHandler = flush(indexBlockBuilder);

        ByteBuffer footer = ByteBuffer.allocate(12);
        indexHandler.encodeFixed(footer);
        footer.rewind();
        fc.write(footer);
    }


    private BlockHandler flush(BlockBuilder blockBuilder) throws IOException {

        BlockHandler handler = null;
        if (!blockBuilder.isEmpty()) {
            byte[] raw = blockBuilder.finish();


            ByteArrayOutputStream baos = new ByteArrayOutputStream(raw.length);
            BlockFormat.encode(raw, Options.BLOCK_COMPRESS_TYPE, baos);

            long offset = fc.position();
            int size = baos.size();
            handler = new BlockHandler(offset, size);
            ByteBuffer buffer = ByteBuffer.allocate(size);
            buffer.put(baos.toByteArray());
            buffer.flip();
            fc.write(buffer);
            blockBuilder.reset();
        }

        return handler;
    }


    @Override
    public void close() throws IOException {
        this.fc.close();
    }

}
