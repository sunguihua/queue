package guihua.sun.queue2;


import java.io.IOException;

public interface Wal extends Iterable<Cell>{

    void append(Cell edition) throws IOException;

}
