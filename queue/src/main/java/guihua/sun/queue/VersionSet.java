package guihua.sun.queue;

import java.io.File;
import java.util.LinkedList;

/**
 * Created by sunguihua on 2019/7/19.
 */
public class VersionSet {

    private volatile Version current;

    private LinkedList<VersionEdit> edits = new LinkedList<>();

    private File manifestFile;

    public VersionSet(Version version) {
        this.current = version;
    }

    public synchronized void append(VersionEdit versionEdit) {

    }

}
