package wal.test;

import guihua.sun.wal.WAL;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by sunguihua on 2019/7/24.
 */
public class WalTest {

    private WAL wal;


    @Before
    public void prepare(){
        String dir = "/Users/sunguihua/temp";
        Path logPath = Paths.get(dir, "wal.log");
        wal = new WAL(logPath);
    }

    @Test
    public void testWrite(){

        try (WAL.Writer writer = wal.writer()) {

            for(int j=0;j<2;j++){
                int random = ThreadLocalRandom.current().nextInt(256);
                System.out.println("=========" + random + "=============");
                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < random; i++) {
                    sb.append(""+random);
                }

                writer.write(sb.toString().getBytes());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testRead(){
        try (WAL.Reader reader = wal.reader()) {

            while (true) {
                try {
                    byte[] bytes = reader.read();
                    System.out.println("===" + new String(bytes));
                } catch (WAL.EofException e) {
                    System.out.println("===end of file ===");
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
