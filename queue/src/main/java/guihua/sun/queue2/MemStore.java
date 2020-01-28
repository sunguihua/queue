package guihua.sun.queue2;

public interface MemStore<K extends Key> {

    public void put(InternelKey<K> ikey,InternelValue value);

    public InternelValue get(K key);
}
