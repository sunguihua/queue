package guihua.sun.utils;

/**
 * Created by sunguihua on 2019/6/6.
 */
public class Slice {

    private byte[] data;
    private int offset;
    private int length;

    private int position;
    private int limit;

    public Slice(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.position = offset;
        this.limit = offset + length;

        assert this.limit <= data.length;
    }

    public Slice(byte[] data) {
        this.data = data;
        this.offset = 0;
        this.position = 0;
        this.length = data.length;
        this.limit = data.length;
    }


    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getLimit() {
        return limit;
    }

    public void rewind() {
        this.position = this.offset;
    }


    private long readUnsignedVarLong(int sizeof) {
        int foo = 0;
        byte b;
        long value = 0l;
        for (; ; this.position++) {

            if (foo > sizeof - 1) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
            if (this.position >= this.limit) {
                throw new IllegalArgumentException("buffer does not contain enough bytes!");
            }
            b = this.data[position];

            if ((b & 0x80) != 0) {
                value |= (b & 0x7F) << foo;
            } else {
                value |= b << foo;
                break;
            }

            foo += 7;
        }

        this.position++;

        return value;
    }


    public long readUnsignedVarLong() {
        return readUnsignedVarLong(Long.SIZE);
    }

    public int readUnsignedVarInt() {
        return (int) readUnsignedVarLong(Integer.SIZE);
    }
}
