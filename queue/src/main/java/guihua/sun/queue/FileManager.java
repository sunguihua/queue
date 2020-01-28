package guihua.sun.queue;

import java.io.File;

/**
 * Created by sunguihua on 2019/8/2.
 */
public interface FileManager {

    KFile create(KFileType fileType);

    KFile get(long fileNumber);

    KFile getManifest();

    void compactManifest();

    void recycle();
}
