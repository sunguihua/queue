package guihua.sun.queue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static guihua.sun.utils.Protocol.*;

/**
 * Created by sunguihua on 2019/7/19.
 */
public class Version<T> implements Cloneable{

    VersionSet vset;
    Config config;
    SLog log;
    List<FileMetaData>[] levels;

    int refs;
    private long seq;

    public synchronized void put(byte[] userKey, byte[] userValue) throws IOException {

        long nextSeq = seq + 1;
        int byteLength = varIntLength(userKey.length) + userKey.length + varLongLength(nextSeq)
                + varIntLength(userValue.length) + userValue.length;

        ByteArrayOutputStream baos = new ByteArrayOutputStream(byteLength);

        writeUnsignedVarInt(userKey.length, baos);
        baos.write(userKey);
        writeUnsignedVarLong(nextSeq);
        writeUnsignedVarInt(userValue.length);
        baos.write(userValue);

        //logWriter.write(baos.toByteArray());
        this.seq ++ ;

        //memTable.put(nextSeq, userKey, userValue);
    }

    public UserValue get(UserKey userKey) {
        return null;
    }


    @Override
    public Object clone() throws CloneNotSupportedException {
        Version version = (Version) super.clone();
        version.refs = 0;
        return version;
    }
}
