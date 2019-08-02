/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package guihua.sun.utils;


import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;


/**
 * <p>Encodes signed and unsigned values using a common variable-length
 * scheme, found for example in
 * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
 * Google's Protocol Buffers</a>. It uses fewer bytes to encode smaller values,
 * but will use slightly more bytes to encode large values.</p>
 * <p/>
 * <p>Signed values are further encoded using so-called zig-zag encoding
 * in order to make them "compatible" with variable-length encoding.</p>
 */
public final class Protocol {

    private Protocol() {
    }

    /**
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. It uses zig-zag encoding to efficiently
     * encode signed values. If values are known to be nonnegative,
     * {@link #writeUnsignedVarLong(long, OutputStream)} should be used.
     *
     * @param value value to encode
     * @param out   to write bytes to
     * @throws IOException if {@link OutputStream} throws {@link IOException}
     */
    public static void writeSignedVarLong(long value, OutputStream out) throws IOException {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeUnsignedVarLong((value << 1) ^ (value >> 63), out);
    }

    /**
     * Encodes a value using the variable-length encoding from
     * <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative.
     * If values can be negative, use {@link #writeSignedVarLong(long, OutputStream)}
     * instead. This method treats negative input as like a large unsigned value.
     *
     * @param value value to encode
     * @param out   to write bytes to
     * @throws IOException if {@link OutputStream} throws {@link IOException}
     */
    public static void writeUnsignedVarLong(long value, OutputStream out) throws IOException {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            out.write(((int) value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write((int) value & 0x7F);
    }


    public static byte[] writeUnsignedVarLong(long value) {
        byte[] byteArrayList = new byte[10];
        int i = 0;
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            byteArrayList[i] = (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        byteArrayList[i] = (byte) (value & 0x7F);
        byte[] out = new byte[i + 1];
        for (; i >= 0; i--) {
            out[i] = byteArrayList[i];
        }
        return out;
    }

    /**
     * @see #writeSignedVarLong(long, OutputStream)
     */
    public static void writeSignedVarInt(int value, OutputStream out) throws IOException {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeUnsignedVarInt((value << 1) ^ (value >> 31), out);
    }

    /**
     * @see #writeUnsignedVarLong(long, OutputStream)
     */
    public static void writeUnsignedVarInt(int value, OutputStream out) throws IOException {
        while ((value & 0xFFFFFF80) != 0L) {
            out.write((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        out.write(value & 0x7F);
    }

    public static byte[] writeSignedVarInt(int value) {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        return writeUnsignedVarInt((value << 1) ^ (value >> 31));
    }

    /**
     * @see #writeUnsignedVarLong(long, OutputStream)
     * <p/>
     * This one does not use streams and is much faster.
     * Makes a single object each time, and that object is a primitive array.
     */
    public static byte[] writeUnsignedVarInt(int value) {
        byte[] byteArrayList = new byte[10];
        int i = 0;
        while ((value & 0xFFFFFF80) != 0L) {
            byteArrayList[i++] = ((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        byteArrayList[i] = ((byte) (value & 0x7F));
        byte[] out = new byte[i + 1];
        for (; i >= 0; i--) {
            out[i] = byteArrayList[i];
        }
        return out;
    }

    public static void writeFixedInt32(int value, OutputStream out) throws IOException {
        out.write(value >> 24);
        out.write(value >> 16);
        out.write(value >> 8);
        out.write(value);
    }

    public static byte[] writeFixedInt32(int value) {
        byte[] byteArrayList = new byte[4];
        byteArrayList[0] = (byte) (value >> 24);
        byteArrayList[1] = (byte) (value >> 16);
        byteArrayList[2] = (byte) (value >> 8);
        byteArrayList[3] = (byte) value;

        return byteArrayList;
    }

    public static int readFixedInt32(byte[] bytes, int a) {
        assert a + 4 <= bytes.length;

        return bytes[a] << 24 | (bytes[a + 1] & 0xff) << 16 | (bytes[a + 2] & 0xff) << 8
                | (bytes[a + 3] & 0xff);
    }

    public static int readFixedInt32(InputStream is) throws IOException {
        byte[] byteArray = new byte[4];
        int len = is.read(byteArray);

        assert len == 4;
        return readFixedInt32(byteArray, 0);
    }

    /**
     * @param in to read bytes from
     * @return decode value
     * @throws IOException              if {@link InputStream} throws {@link IOException}
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 9 bytes have been read
     * @see #writeSignedVarLong(long, OutputStream)
     */
    public static long readSignedVarLong(InputStream in) throws IOException {
        long raw = readUnsignedVarLong(in);
        // This undoes the trick in writeSignedVarLong()
        long temp = (((raw << 63) >> 63) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1L << 63));
    }

    /**
     * @param in to read bytes from
     * @return decode value
     * @throws IOException              if {@link InputStream} throws {@link IOException}
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 9 bytes have been read
     * @see #writeUnsignedVarLong(long, OutputStream)
     */
    public static long readUnsignedVarLong(InputStream in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;
        while (((b = in.read()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }


    public static long readUnsignedVarLong(byte[] buffer, int a) {

        int i = 0;
        int foo = 0;
        byte b;
        long value = 0l;
        for (; ; i++) {

            if (foo > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
            if (a + i >= buffer.length) {
                throw new IllegalArgumentException("buffer does not contain enough bytes!");
            }
            b = buffer[a + i];

            if ((b & 0x80) != 0) {
                value |= (b & 0x7F) << foo;
            } else {
                value |= b << foo;
                break;
            }

            foo += 7;
        }

        return value;
    }


    /**
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     * @throws IOException              if {@link InputStream} throws {@link IOException}
     * @see #readSignedVarLong(InputStream)
     */
    public static int readSignedVarInt(InputStream in) throws IOException {
        int raw = readUnsignedVarInt(in);
        // This undoes the trick in writeSignedVarInt()
        int temp = (((raw << 31) >> 31) ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & (1 << 31));
    }

    /**
     * @throws IllegalArgumentException if variable-length value does not terminate
     *                                  after 5 bytes have been read
     * @throws IOException              if {@link InputStream} throws {@link IOException}
     * @see #readUnsignedVarLong(InputStream)
     */
    public static int readUnsignedVarInt(InputStream in) throws IOException {
        int value = 0;
        int i = 0;
        int b;
        while (((b = in.read()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 35) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        return value | (b << i);
    }

    public static int readUnsignedVarInt(byte[] bytes, int position, int[] value) {
        value[0] = 0;
        int i = 0;
        int j = position;
        byte b = Byte.MIN_VALUE;
        for (; j < bytes.length; j++) {
            b = bytes[j];
            if ((b & 0x80) == 0) {
                j++;
                break;
            }
            value[0] |= (b & 0x7f) << i;
            i += 7;
            if (i > 31) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        value[0] |= (b << i);
        return j;
    }

    public static long readUnsignedVarLong(byte[] bytes, int position, long[] value) {
        value[0] = 0l;
        int i = 0;
        byte b = Byte.MIN_VALUE;
        int j = position;
        for (; j < bytes.length; j++) {
            b = bytes[j];
            if ((b & 0x80) == 0) {
                j++;
                break;
            }
            value[0] |= (b & 0x7f) << i;
            i += 7;
            if (i > 63) {
                throw new IllegalArgumentException("Variable length quantity is too long");
            }
        }
        value[0] |= (b << i);
        return j;
    }

    public static int varIntLength(int v) {
        int length = 2;
        while (v > 128) {
            v >>= 7;
            length++;
        }
        return length;
    }

    public static int varLongLength(long v) {
        int length = 2;
        while (v > 128) {
            v >>= 7;
            length++;
        }
        return length;
    }

    public static void main(String[] args) {
        byte[] a = Protocol.writeUnsignedVarLong(29);

        for (byte bt : a) {
            System.out.print(Integer.toBinaryString(bt));
        }
        System.out.println();
        System.out.println(Integer.toBinaryString(29));
        System.out.println(Protocol.readUnsignedVarLong(a, 0));
    }
}