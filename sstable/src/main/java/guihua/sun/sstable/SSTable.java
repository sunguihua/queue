package guihua.sun.sstable;

import guihua.sun.utils.Slice;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;

import static java.nio.file.StandardOpenOption.*;

/**
 * Created by sunguihua on 2019/5/29.
 */
public class SSTable {

    private FileChannel fc;

    private BlockIndex index;


    public SSTable(Path path) throws IOException {
        File file = path.toFile();
        assert file.exists() : "文件不存在";

        fc = FileChannel.open(path, READ);
        long size = fc.size();
        ByteBuffer footer = ByteBuffer.allocate(12);
        fc.read(footer, size - 12);

        BlockHandler indexBlockHandler = BlockHandler.decodeFixed(footer);


        Block indexBlock = readBlock(indexBlockHandler);

        Iterator<Record> it = indexBlock.iterator();
        this.index = new BlockIndex(it, Options.COMPARATOR);
    }


    private Block readBlock(BlockHandler handler) throws IOException {
        long position = handler.getOffset();
        int size = handler.getSize();
        ByteBuffer blockBuffer = ByteBuffer.allocate(size);
        if (size != fc.read(blockBuffer, position)) {
            throw new IllegalStateException("sstable格式异常");
        }

        BlockFormat blockFormat = BlockFormat.decode(blockBuffer.array());
        return new Block(blockFormat.getData(), Options.BLOCK_RESTART_POINT_INTERVAL, Options.COMPARATOR);
    }

    public Record seek(Slice key) throws IOException {

        Optional<BlockHandler> optional = index.seek(key);
        if (!optional.isPresent()) {
            return null;
        }

        BlockHandler handler = optional.get();
        Block block = readBlock(handler);
        return block.seek(key);
    }

}
