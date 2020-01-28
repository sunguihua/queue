package guihua.sun.queue2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Key {

    void write(OutputStream os) throws IOException;

    void read(InputStream is) throws IOException;
}
