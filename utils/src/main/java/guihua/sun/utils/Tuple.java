package guihua.sun.utils;

/**
 * Created by sunguihua on 2019/6/11.
 */
public class Tuple<K,V> {
    private final K k;
    private final V v;

    public Tuple(K k, V v) {
        this.k = k;
        this.v = v;
    }

    public K _1(){
        return k;
    }

    public V _2(){
        return v;
    }
}