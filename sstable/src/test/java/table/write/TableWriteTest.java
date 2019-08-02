package table.write;

import guihua.sun.sstable.Options;
import guihua.sun.sstable.Record;
import guihua.sun.sstable.SSTable;
import guihua.sun.sstable.SSTableBuilder;
import guihua.sun.utils.*;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by sunguihua on 2019/6/11.
 */
public class TableWriteTest {

    String dir = "/Users/sunguihua/temp/table";

    @Test
    public void testWrite() {

        try (SSTableBuilder builder = new SSTableBuilder(Paths.get(dir, "1.sst"))) {

            List<Tuple<Slice, Slice>> list = new LinkedList<>();
            for (int i = 0; i < 1000000; i++) {
                list.add(new Tuple<>(new IntegerSlice(i), new StringSlice("value" + i)));
            }

            Collections.sort(list, new Comparator<Tuple<Slice, Slice>>() {
                @Override
                public int compare(Tuple<Slice, Slice> o1, Tuple<Slice, Slice> o2) {
                    return Options.COMPARATOR.compare(o1._1(), o2._1());
                }
            });


            list.forEach(
                    sliceSliceTuple -> {
                        try {
                            builder.append(sliceSliceTuple._1().getData(), sliceSliceTuple._2().getData());
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
            );

            builder.finish();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRead() {
        try {
            SSTable ssTable = new SSTable(Paths.get(dir, "1.sst"));

            Random random = new Random(100);
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                int foo = random.nextInt(100000);
                System.out.println(foo);
                Record r = ssTable.seek(new IntegerSlice(foo));
                Slice slice = r.getValue();
                System.out.println(new String(slice.getData(), slice.getOffset(), slice.getLength()));

            }
            long endTime = System.currentTimeMillis();

            System.out.println("耗时:" + (endTime - startTime) + "ms");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
