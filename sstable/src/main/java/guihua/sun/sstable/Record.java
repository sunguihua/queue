package guihua.sun.sstable;

import guihua.sun.utils.Slice;

/**
 * Created by sunguihua on 2019/5/29.
 */
public class Record {
    private Slice key;
    private Slice value;

    public Slice getKey() {
        return key;
    }

    public Slice getValue() {
        return value;
    }

    public void setKey(Slice key) {
        this.key = key;
    }

    public void setValue(Slice value) {
        this.value = value;
    }
}
