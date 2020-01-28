package guihua.sun.queue2;

import guihua.sun.queue.SLog;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;

public class DefaultWal implements Wal {

    private Path file;
    private SLog log ;
    private SLog.Writer writer;


    public DefaultWal(Path file) throws IOException {
        this.file = file;
        log = new SLog(file);
        this.writer = log.writer();
    }


    @Override
    public void append(Cell edition) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        edition.write(baos);
        writer.write(baos.toByteArray());
    }

    @Override
    public Iterator<Cell> iterator() {
        try {
            SLog.Reader reader =log.reader();
            return new CellIterator(reader);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }

    }

    class CellIterator implements Iterator<Cell> {

        private final SLog.Reader reader;
        private byte[] data;

        public CellIterator(SLog.Reader reader) {
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            try {
                this.data = reader.read();
                return true;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SLog.EofException e) {
                return false;
            }
            return false;
        }

        @Override
        public Cell next() {
            Cell cell = new Cell();
            try {
                cell.read(new ByteArrayInputStream(this.data));
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
            return cell;
        }
    };
}
