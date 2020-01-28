package guihua.sun.queue2;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DefaultMemTable<K extends Key> implements MemStore<K> {

    private SortedMap<InternelKey<K>,InternelValue> map;

    public DefaultMemTable(Comparator<InternelKey<K>> comparator) {
        this.map = new ConcurrentSkipListMap<>(comparator);
    }

    @Override
    public void put(InternelKey<K> ikey, InternelValue value) {
        map.put(ikey,value);
    }

    @Override
    public InternelValue get(K key) {
        SortedMap<InternelKey<K>,InternelValue> subMap = map.subMap(new InternelKey<K>(0,key),new InternelKey<>(Long.MAX_VALUE,key));
        if(subMap == null || subMap.size() ==0){
            return null;
        }

        return subMap.entrySet().iterator().next().getValue();
    }


}
