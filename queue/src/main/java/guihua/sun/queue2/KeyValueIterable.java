package guihua.sun.queue2;

public interface KeyValueIterable {


    void reset(Object key);

    Object next();
    boolean hasNext();
}
