package guihua.sun.sstable;

import guihua.sun.utils.CompressionFactory;
import guihua.sun.utils.Decompressor;
import guihua.sun.utils.Protocol;
import guihua.sun.utils.Slice;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Comparator;
import java.util.Iterator;
import java.util.zip.CRC32;

/**
 * Created by sunguihua on 2019/5/30.
 */
public class Block {

    private final byte[] raw;
    private ByteBuffer buffer;
    private final int restartPointInterval;

    private int dataLength;
    private int rpSize;
    private Integer[] rpOffsets;

    private Comparator<Slice> comparator;

    public Block(byte[] raw, int restartPointInterval, Comparator<Slice> comparator) {
        this.raw = raw;
        this.restartPointInterval = restartPointInterval;

        int rpSizeOffset = raw.length - 4;

        assert rpSizeOffset > 0 : "长度不合法";
        this.rpSize = Protocol.readFixedInt32(raw, rpSizeOffset);

        int rpOffsetBegin = rpSizeOffset - 4 * rpSize;

        assert rpOffsetBegin > 0 : "长度不合法";

        this.dataLength = rpOffsetBegin;
        this.rpOffsets = new Integer[this.rpSize];
        for (int i = 0; i < rpSize; i++) {
            rpOffsets[i] = Protocol.readFixedInt32(raw, rpOffsetBegin + 4 * i);
        }
        this.comparator = comparator;
    }

    private int decode(Slice lastKey, int position, final Record record) {
        int[] sharedKeyLength = new int[1];
        int[] unsharedKeyLength = new int[1];
        int[] valueLength = new int[1];

        position = Protocol.readUnsignedVarInt(raw, position, sharedKeyLength);
        position = Protocol.readUnsignedVarInt(raw, position, unsharedKeyLength);
        position = Protocol.readUnsignedVarInt(raw, position, valueLength);

        Slice key, value;
        if (sharedKeyLength[0] > 0) {
            byte[] keyData = new byte[unsharedKeyLength[0] + sharedKeyLength[0]];
            System.arraycopy(lastKey.getData(), lastKey.getOffset(), keyData, 0, sharedKeyLength[0]);
            System.arraycopy(raw, position, keyData, sharedKeyLength[0], unsharedKeyLength[0]);
            key = new Slice(keyData, 0, keyData.length);
        } else {
            key = new Slice(raw, position, unsharedKeyLength[0]);
        }
        position += unsharedKeyLength[0];
        value = new Slice(raw, position, valueLength[0]);
        position += valueLength[0];
        record.setKey(key);
        record.setValue(value);
        return position;
    }

    public Iterator<Record> iterator() {
        return iterator(0, this.dataLength);
    }

    private Iterator<Record> iterator(int offset, int length) {

        return new Iterator<Record>() {
            int position = offset;
            int limit = position + length;
            int curOffset = 0;
            Slice lastKey;
            Record record = new Record();

            @Override
            public boolean hasNext() {
                return limit > position;
            }

            @Override
            public Record next() {

                this.position = decode(lastKey, position, record);
                this.lastKey = record.getKey();
                return record;
            }
        };
    }

    public Record seek(Slice key) {

        Record recordMin = new Record();
        Record recordMax = new Record();

        int indexMax = rpSize - 1;
        decode(null, rpOffsets[0], recordMin);
        decode(null, rpOffsets[indexMax], recordMax);

        if (comparator.compare(key, recordMin.getKey()) < 0) {
            return null;
        }
        int rpIndex = seek(0, indexMax, key);
        int length = 0;
        if(rpIndex+1 == rpSize){
            length = this.dataLength - rpOffsets[rpIndex];
        }else{
            length = rpOffsets[rpIndex + 1] - rpOffsets[rpIndex];
        }

        Iterator<Record> it = iterator(rpOffsets[rpIndex], length);

        while (it.hasNext()) {
            Record record = it.next();

            if (comparator.compare(key, record.getKey()) == 0) {
                return record;
            } else {
                continue;
            }
        }
        return null;
    }

    private int seek(int a, int b,  Slice key) {
        if (a == b) {
            return a;
        }

        int mid = (a + b) / 2 + 1;
        Record recordMid = new Record();
        decode(null, rpOffsets[mid], recordMid);

        int compare = comparator.compare(key, recordMid.getKey());
        if (compare < 0) {
            return seek(a, mid-1, key);
        } else if (compare > 0) {
            return seek(mid, b, key);
        } else {
            return mid;
        }
    }
}
