package guihua.sun.queue;

import guihua.sun.utils.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;

/**
 * Created by sunguihua on 2019/7/22.
 */
public class Current {

    private final static Logger LOG = LoggerFactory.getLogger(Current.class);

    private final static String _fileName = "current";

    File home;
    File currentFile;

    public Current(File home) {

        if (!home.isDirectory()) {
            throw new IllegalArgumentException("home目录不是文件夹");
        }
        this.home = home;

        File[] files = home.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (_fileName.equals(name)) {
                    return true;
                }
                return false;
            }
        });
        if (files == null) {
            LOG.debug("current文件不存在");
            create();
        }

        if (files.length > 1) {
            LOG.error("存在多个current文件!");
        }
        this.currentFile = files[0];
    }


    private void create() {
        this.currentFile = new File(this.home.getPath(), _fileName);
        LOG.info("创建current文件");
    }


    public long readManifestFileNo() throws IOException {

        try (InputStream is = new FileInputStream(this.currentFile)) {

            if(is.available() < 2){
                throw new IllegalStateException("current文件内容异常:小于2个字节");
            }
            return Protocol.readUnsignedVarLong(is);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("current文件不存在", e);
        }
    }

    public void updateManifestFileNo(long manifestFileNo) throws IOException {
        try (OutputStream os = new FileOutputStream(this.currentFile)) {
            Protocol.writeUnsignedVarLong(manifestFileNo);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("current文件不存在", e);
        }
    }
}
