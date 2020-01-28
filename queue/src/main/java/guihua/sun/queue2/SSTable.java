package guihua.sun.queue2;

public interface SSTable {

    public InternelValue get(Key key);

    Cell getCell(Key key);
}
