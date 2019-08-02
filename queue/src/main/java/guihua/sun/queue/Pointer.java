package guihua.sun.queue;

import java.io.Closeable;

/**
 * Created by sunguihua on 2019/7/7.
 */
public interface Pointer extends Closeable {

    Entry pull();

    Entry peek();

    void commit(long seqNo);
}
