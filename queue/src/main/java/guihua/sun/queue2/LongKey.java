package guihua.sun.queue2;

import guihua.sun.utils.Protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class LongKey implements Key {

    private long value;

    public LongKey(long value) {
        this.value = value;
    }

    public LongKey() {
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    @Override
    public void write(OutputStream os) throws IOException {
        Protocol.writeUnsignedVarLong(this.value,os);
    }

    @Override
    public void read(InputStream is) throws IOException {
        this.value = Protocol.readUnsignedVarLong(is);
    }
}
