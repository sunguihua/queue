package guihua.sun.queue;

/**
 * Created by sunguihua on 2019/7/7.
 */
public class Queue {



    FileManager fileManager;
    VersionSet versionSet;

    MemTable m1;
    MemTable m2;



    private Queue(String  homePath,Config config){
        this.fileManager = fileManager;
        FileManager fileManager = new DefaultFileManager(homePath);
        KFile manifest = fileManager.getManifest();
        if(manifest == null){
            //create
        }else{
            // init from an exited one
        }
    }

    /**
     * 创建一个新的queue。
     * 主要包括 ①创建一个manifest文件->②创建wal和memtable->③将初始化对应的versionEdit追加到当前"空"的VersionSet中，
     * 同时也就有了一个初始的version
     */
    private void create(){
        //在这里区别于levelDb，将不再这一层对current文件进行管理，而是把current文件的管理屏蔽在FileManager中
        KFile manifest = fileManager.create(KFileType.Manifest);
        this.versionSet = new VersionSet(manifest);

        KFile wal = fileManager.create(KFileType.Log);
        this.m1 = new DefaultMemTable(wal);

        VersionEdit versionEdit = new VersionEdit();
        versionEdit.setLogNumber(wal.getNumber());
        versionSet.append(versionEdit);
    }

    /**
     * 对已有的queue进行初始化。主要包括
     * ①加载manifest文件，依次追加manifest文件中每个VersionEdit到VersionSet，完成对VersionSet的初始化->②回收（删除）无引用的文件
     */
    private void initFromExisted(KFile manifest){


    }



}
