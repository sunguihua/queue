package guihua.sun.test;

import guihua.sun.queue2.Key;
import guihua.sun.queue2.LongKey;
import guihua.sun.queue2.Store;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Comparator;

public class StoreTest {

    private final static String homePath = "/Users/sunguihua/queue";

    @Test
    public void startStore() {
        Store store = new Store(Paths.get(homePath), new LongKeyComparator());
        try {
            store.start();

            store.put(new LongKey(100), "v100".getBytes());

            byte[] value = store.get(new LongKey(100));
            System.out.println("====================>" + new String(value));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private class LongKeyComparator implements Comparator<LongKey> {

        @Override
        public int compare(LongKey o1, LongKey o2) {
            return (int) (o1.getValue() - o2.getValue());
        }
    }
}
