package guihua.sun.queue2;

import guihua.sun.utils.Protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class InternelValue {

    private int action;
    private byte[] data;

    public InternelValue() {
    }

    public InternelValue(int action, byte[] data) {
        this.action = action;
        this.data = data;
    }

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void write(OutputStream os) throws IOException {
        Protocol.writeUnsignedVarInt(this.action,os);
        os.write(data);
    }

    public void read(InputStream is) throws IOException{
        this.action = Protocol.readUnsignedVarInt(is);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int lenght = 0;
        while((lenght = is.read(buffer))>0){
            baos.write(buffer,0,lenght);
        }
        this.data = baos.toByteArray();
    }
}
