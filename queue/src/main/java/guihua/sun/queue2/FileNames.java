package guihua.sun.queue2;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileNames {

    public final static String CURRENT = "current";

    public final static String MANIFEST_SUFFIX = ".manifest";
    public final static String WAL_SUFFIX = ".log";
    public final static String SSTABLE_SUFFIX = ".sst";

    public static String manifest(long num){
        return String.valueOf(num) + MANIFEST_SUFFIX;
    }

    public static String wal(long num){
        return String.valueOf(num) + WAL_SUFFIX;
    }

    public static String sstable(long num){
        return String.valueOf(num) + SSTABLE_SUFFIX;
    }

    public static Path current(Path home) {
        return home.resolve(CURRENT);
    }

    public static Path getManifestFile(Path home, long num,boolean createIfNotExist) throws IOException {
        Path file = home.resolve(FileNames.manifest(num));
        if(createIfNotExist && Files.exists(file)){
            Files.createFile(file);
        }

        return file;
    }

    public static Path getWal(Path home, long num,boolean createIfNotExist) throws IOException {
        Path file = home.resolve(FileNames.wal(num));
        if(createIfNotExist && Files.exists(file)){
            Files.createFile(file);
        }

        return file;
    }

    public static Path getSStable(Path home, long num,boolean createIfNotExist) throws IOException {
        Path file = home.resolve(FileNames.sstable(num));
        if(createIfNotExist && Files.exists(file)){
            Files.createFile(file);
        }
        return file;
    }
}
