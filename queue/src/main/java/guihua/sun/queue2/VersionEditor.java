package guihua.sun.queue2;

import java.nio.file.Path;
import java.util.List;

public class VersionEditor {

    private List<SSTableMeta> newTables;

    private List<SSTableMeta> sourceTables;

    private Path log;
    private Path prioLog;

    private Long maxSeqNo;

    public Long getMaxSeqNo() {
        return maxSeqNo;
    }

    public void setMaxSeqNo(Long maxSeqNo) {
        this.maxSeqNo = maxSeqNo;
    }

    public List<SSTableMeta> getNewTables() {
        return newTables;
    }

    public void setNewTables(List<SSTableMeta> newTables) {
        this.newTables = newTables;
    }

    public List<SSTableMeta> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(List<SSTableMeta> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public Path getLog() {
        return log;
    }

    public void setLog(Path log) {
        this.log = log;
    }

    public Path getPrioLog() {
        return prioLog;
    }

    public void setPrioLog(Path prioLog) {
        this.prioLog = prioLog;
    }
}
