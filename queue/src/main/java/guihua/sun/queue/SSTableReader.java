package guihua.sun.queue;

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by sunguihua on 2019/7/7.
 */
public interface SSTableReader extends Closeable {

    SSTable getSSTable();

    boolean readBlock(final LinkedList<Entry> cache);

    void setPointer(byte key, long seqNo);

}
