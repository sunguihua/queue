package guihua.sun.queue2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Cell {
    InternelKey internelKey;
    InternelValue internelValue;

    public Cell() {
    }

    public Cell(InternelKey internelKey, InternelValue internelValue) {
        this.internelKey = internelKey;
        this.internelValue = internelValue;
    }

    public InternelKey getInternelKey() {
        return internelKey;
    }

    public void setInternelKey(InternelKey internelKey) {
        this.internelKey = internelKey;
    }

    public InternelValue getInternelValue() {
        return internelValue;
    }

    public void setInternelValue(InternelValue internelValue) {
        this.internelValue = internelValue;
    }

    public void write(OutputStream os) throws IOException {
        this.internelKey.write(os);
        this.internelValue.write(os);
    }

    public void read(InputStream is) throws IOException{
        this.internelKey = new InternelKey();
        internelKey.read(is);

        this.internelValue = new InternelValue();
        this.internelValue.read(is);
    }
}
