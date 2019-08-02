package guihua.sun.queue;

import java.io.File;

/**
 * Created by sunguihua on 2019/8/2.
 */
public interface KFile {

    long getNumber();

    KFileType getType();

    void ref();

    void unref();

    File getFile();

    void mark();

    boolean isLive();

}
