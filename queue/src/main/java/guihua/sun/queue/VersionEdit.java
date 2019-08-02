package guihua.sun.queue;


import guihua.sun.queue.codec.CodecException;
import guihua.sun.queue.codec.protocol.*;
import guihua.sun.queue.codec.ICodec;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sunguihua on 2019/7/19.
 * <p>
 * 一次VersionEdit要么是一次MinorCompaction，要么是一次MajorCompaction
 */
public class VersionEdit implements ICodec {


    private final static TStruct versionEditStruct = new TStruct("VersionEdit");
    private final static TField _comparator = new TField("comparator", TType.STRING, (short) 1);
    private final static TField _preLogNumber = new TField("preLogNumber", TType.I64, (short) 2);
    private final static TField _logNumber = new TField("logNumber", TType.I64, (short) 3);
    private final static TField _nextFileNumber = new TField("nextFileNumber", TType.I64, (short) 4);
    private final static TField _lastSeqNumber = new TField("lastSeqNumber", TType.I64, (short) 5);
    private final static TField _newFiles = new TField("newFiles", TType.LIST, (short) 6);
    private final static TField _delFiles = new TField("delFiles", TType.LIST, (short) 7);

    private String comparatorName;
    private boolean hasComparator;

    private long preLogNumber;
    private boolean hasPreLogNumber;

    private long logNumber;
    private boolean hasLogNumber;

    private long nextFileNumber;
    private boolean hasNextFileNumber;

    private long lastSeqNumber;
    private boolean hasLastSeqNumber;


    private List<FileMetaData> newFiles;
    private boolean hasNewFiles;

    private List<FileMetaData> delFiles;
    private boolean hasDelFiles;

    @Override
    public void read(TProtocol iproto) throws CodecException {

        iproto.readStructBegin();
        while (true) {
            TField fieldScheme = iproto.readFieldBegin();
            if (fieldScheme.type == TType.STOP) {
                break;
            }

            switch (fieldScheme.id) {
                case 1: // compartore
                    if (fieldScheme.type == TType.STRING) {
                        this.comparatorName = iproto.readString();
                        this.hasComparator = true;
                    } else {
                        TProtocolUtil.skip(iproto, fieldScheme.type);
                    }
                    break;
                case 2: //preLogNumber
                    if (fieldScheme.type == TType.I64) {
                        this.preLogNumber = iproto.readI64();
                        this.hasPreLogNumber = true;
                    } else {
                        TProtocolUtil.skip(iproto, fieldScheme.type);
                    }
                    break;
                case 3: //logNumber
                    if (fieldScheme.type == TType.I64) {
                        this.logNumber = iproto.readI64();
                        this.hasLogNumber = true;
                    } else {
                        TProtocolUtil.skip(iproto, fieldScheme.type);
                    }
                    break;
                case 4: //nextFileNumber
                    if (fieldScheme.type == TType.I64) {
                        this.nextFileNumber = iproto.readI64();
                        this.hasNextFileNumber = true;
                    } else {
                        TProtocolUtil.skip(iproto, fieldScheme.type);
                    }
                    break;
                case 5: //nextFileNumber
                    if (fieldScheme.type == TType.I64) {
                        this.lastSeqNumber = iproto.readI64();
                        this.hasLastSeqNumber = true;
                    } else {
                        TProtocolUtil.skip(iproto, fieldScheme.type);
                    }
                    break;
                case 6: //newFiles
                    if (fieldScheme.type == TType.LIST) {
                        TList listScheme = iproto.readListBegin();
                        this.newFiles = new ArrayList<>(listScheme.size);
                        this.hasNewFiles = true;
                        for (int i = 0; i < listScheme.size; i++) {
                            FileMetaData fileMetaData = new FileMetaData();
                            fileMetaData.read(iproto);
                            this.newFiles.add(fileMetaData);
                        }
                        iproto.readListEnd();
                    }
            }
            iproto.readFieldEnd();
        }
        iproto.readStructEnd();
    }

    @Override
    public void write(TProtocol oproto) throws CodecException {
        oproto.writeStructBegin(versionEditStruct);

        if (hasComparator) {
            oproto.writeFieldBegin(_comparator);
            oproto.writeString(this.comparatorName);
            oproto.writeFieldEnd();
        }

        if (hasPreLogNumber) {
            oproto.writeFieldBegin(_preLogNumber);
            oproto.writeI64(this.preLogNumber);
            oproto.writeFieldEnd();
        }

        if (hasLogNumber) {
            oproto.writeFieldBegin(_logNumber);
            oproto.writeI64(this.logNumber);
            oproto.writeFieldEnd();
        }


        if (hasNextFileNumber) {
            oproto.writeFieldBegin(_nextFileNumber);
            oproto.writeI64(this.nextFileNumber);
            oproto.writeFieldEnd();
        }

        if (hasLastSeqNumber) {
            oproto.writeFieldBegin(_lastSeqNumber);
            oproto.writeI64(lastSeqNumber);
            oproto.writeFieldEnd();
        }

        if (hasNewFiles) {
            oproto.writeFieldBegin(_newFiles);
            oproto.writeListBegin(new TList(TType.STRUCT, this.newFiles.size()));
            for (FileMetaData fileMetaData : this.newFiles) {
                fileMetaData.write(oproto);
            }
            oproto.writeListEnd();
            oproto.writeFieldEnd();
        }

        if (hasDelFiles) {
            oproto.writeFieldBegin(_delFiles);
            oproto.writeListBegin(new TList(TType.STRUCT, this.delFiles.size()));
            for (FileMetaData fileMetaData : this.delFiles) {
                fileMetaData.write(oproto);
            }
            oproto.writeListEnd();
            oproto.writeFieldEnd();
        }

        oproto.writeStructEnd();
    }

    public String getComparatorName() {
        return comparatorName;
    }

    public void setComparatorName(String comparatorName) {
        this.comparatorName = comparatorName;
    }

    public boolean isHasComparator() {
        return hasComparator;
    }

    public void setHasComparator(boolean hasComparator) {
        this.hasComparator = hasComparator;
    }

    public long getPreLogNumber() {
        return preLogNumber;
    }

    public void setPreLogNumber(long preLogNumber) {
        this.preLogNumber = preLogNumber;
    }

    public boolean isHasPreLogNumber() {
        return hasPreLogNumber;
    }

    public void setHasPreLogNumber(boolean hasPreLogNumber) {
        this.hasPreLogNumber = hasPreLogNumber;
    }

    public long getLogNumber() {
        return logNumber;
    }

    public void setLogNumber(long logNumber) {
        this.logNumber = logNumber;
    }

    public boolean isHasLogNumber() {
        return hasLogNumber;
    }

    public void setHasLogNumber(boolean hasLogNumber) {
        this.hasLogNumber = hasLogNumber;
    }

    public long getNextFileNumber() {
        return nextFileNumber;
    }

    public void setNextFileNumber(long nextFileNumber) {
        this.nextFileNumber = nextFileNumber;
    }

    public boolean isHasNextFileNumber() {
        return hasNextFileNumber;
    }

    public void setHasNextFileNumber(boolean hasNextFileNumber) {
        this.hasNextFileNumber = hasNextFileNumber;
    }

    public long getLastSeqNumber() {
        return lastSeqNumber;
    }

    public void setLastSeqNumber(long lastSeqNumber) {
        this.lastSeqNumber = lastSeqNumber;
    }

    public boolean isHasLastSeqNumber() {
        return hasLastSeqNumber;
    }

    public void setHasLastSeqNumber(boolean hasLastSeqNumber) {
        this.hasLastSeqNumber = hasLastSeqNumber;
    }

    public List<FileMetaData> getNewFiles() {
        return newFiles;
    }

    public void setNewFiles(List<FileMetaData> newFiles) {
        this.newFiles = newFiles;
    }

    public List<FileMetaData> getDelFiles() {
        return delFiles;
    }

    public void setDelFiles(List<FileMetaData> delFiles) {
        this.delFiles = delFiles;
    }

}
