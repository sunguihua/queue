package guihua.sun.queue2;

import guihua.sun.queue.FileMetaData;
import guihua.sun.queue.VersionEdit;

import java.nio.file.Path;
import java.util.List;

public interface StoreMeta {

    long getMaxSeqNo();

    long getNextFileNo();

    public List<FileMetaData> getSuitedTables(Key key);

    void append(VersionEdit ve,boolean write);


    long getLogNo();

    long getPreLogNo();
}
