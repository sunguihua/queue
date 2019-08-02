package guihua.sun.queue;

import guihua.sun.queue.codec.transport.TTransport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by sunguihua on 2019/7/22.
 */
public interface Codec<T> {

    public void encode(TTransport transport) throws IOException;

    public T decode(TTransport transport) throws IOException;
}
