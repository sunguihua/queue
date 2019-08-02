package guihua.sun.queue.codec;

/**
 * Created by sunguihua on 2019/7/23.
 */
public class CodecException extends Exception {

    private static final long serialVersionUID = 1L;

    public CodecException() {
    }

    public CodecException(String message) {
        super(message);
    }

    public CodecException(Throwable cause) {
        super(cause);
    }

    public CodecException(String message, Throwable cause) {
        super(message, cause);
    }
}
