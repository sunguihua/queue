package guihua.sun.queue.event;

import guihua.sun.queue.SSTable;

import java.util.List;

/**
 * Created by sunguihua on 2019/7/7.
 */
public class MajorCompactEvent {

    private List<SSTable> delSSTables;
    private List<SSTable> addSSTables;

    public List<SSTable> getDelSSTables() {
        return delSSTables;
    }

    public void setDelSSTables(List<SSTable> delSSTables) {
        this.delSSTables = delSSTables;
    }

    public List<SSTable> getAddSSTables() {
        return addSSTables;
    }

    public void setAddSSTables(List<SSTable> addSSTables) {
        this.addSSTables = addSSTables;
    }
}
