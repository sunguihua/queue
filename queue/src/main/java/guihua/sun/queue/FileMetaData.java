package guihua.sun.queue;

import guihua.sun.queue.codec.CodecException;
import guihua.sun.queue.codec.protocol.TProtocol;
import guihua.sun.queue.codec.ICodec;

/**
 * Created by sunguihua on 2019/7/22.
 */
public class FileMetaData implements ICodec {

    private long fnumber;
    private long fsize;
    private InternalKey smallest;
    private InternalKey largest;

    private int refs;

    public void read(TProtocol iproto) throws CodecException {
    }

    public void write(TProtocol oproto) throws CodecException {
    }

}
