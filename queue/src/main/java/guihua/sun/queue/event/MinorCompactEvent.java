package guihua.sun.queue.event;

import guihua.sun.queue.DefaultMemTable;
import guihua.sun.queue.SSTable;

/**
 * Created by sunguihua on 2019/7/7.
 */
public class MinorCompactEvent {

    private DefaultMemTable newMemTable;

    private SSTable mewSStable;

    public DefaultMemTable getNewMemTable() {
        return newMemTable;
    }

    public void setNewMemTable(DefaultMemTable newMemTable) {
        this.newMemTable = newMemTable;
    }

    public SSTable getMewSStable() {
        return mewSStable;
    }

    public void setMewSStable(SSTable mewSStable) {
        this.mewSStable = mewSStable;
    }
}
