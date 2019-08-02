package guihua.sun.queue;

import java.util.List;

/**
 * Created by sunguihua on 2019/7/7.
 */
public interface LevelReader {

    List<Entry> read();

    void set(byte[] key, long seqNo);
}
