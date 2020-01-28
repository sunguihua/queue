package guihua.sun.queue2;

import guihua.sun.utils.Protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class InternelKey<K extends Key> {

    private long seqNo;
    private K key;

    public InternelKey() {
    }

    public InternelKey(long seqNo, K key) {
        this.seqNo = seqNo;
        this.key = key;
    }

    public long getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(long seqNo) {
        this.seqNo = seqNo;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void write(OutputStream os) throws IOException {
        this.key.write(os);
        Protocol.writeUnsignedVarLong(this.seqNo,os);
    }

    public void read(InputStream is) throws IOException {
        key = (K) new LongKey();
        key.read(is);
        this.seqNo = Protocol.readUnsignedVarLong(is);
    }
}
