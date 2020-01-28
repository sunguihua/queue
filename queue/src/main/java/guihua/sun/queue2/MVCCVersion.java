package guihua.sun.queue2;

/**
 *
 * 1、存储的kv是有版本的
 * 2、磁盘文件列表镜像是有版本的。会者说VersionSet的版本，或者说最大sstable文件的编号
 *
 */
public interface MVCCVersion {

    long getVersion();
}
