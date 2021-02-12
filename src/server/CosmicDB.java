//package server;
//
//import java.io.File;
//
//import buffer.BufferManager;
//import file.FileManager;
//import log.LogFileManager;
//
//public class CosmicDB {
//    public static int BLOCK_SIZE = 400;
//    public static int BUFFER_SIZE = 8;
//    public static String LOGFILE = "CosmicDB.log";
//
//    private FileManager fm;
//    private LogFileManager lfm;
//    private BufferManager bm;
//
////    private MetaDataManager mdm;
////    private ExecutorManager em;
////    private TransactionManager tm;
//
//    /**
//     * Constructor Function
//     * @param dirname
//     * @param blocksize
//     * @param bufsize
//     */
//    public CosmicDB(String dirname, int blocksize, int bufsize){
//        File dbDirectory = new File(dirname);
//        fm = new FileManager(dbDirectory, blocksize);
//        lfm = new LogFileManager(fm, LOGFILE);
//        bm = new BufferManager(fm, lfm, bufsize);
//    }
//
//    public CosmicDB(String dirname){
//        this(dirname, BLOCK_SIZE, BUFFER_SIZE);
//        //创建事务 事务语句原子执行，不可被中断，目前单线程，所以不考虑这个
//        //Transaction tx = new newTx();
//        boolean newdb = fm.create();
//        if(newdb){
//            System.out.println("Create New DataBase Successful");
//        }else{
//            System.out.println("Recover Existed DataBase");
//            tx.recover();
//        }
//        mdm = new MetaDataManager(newdb, tx);
//        QueryExecutor qE = new QueryExecutor(mdm);
//        UpdateExecutor uE = new UpdateExecutor(mdm);
//
//        em = new ExecutorManager(qE, uE);
//        // tx.commit();
//    }
//
//    public Transaction newTx(){
//        return new Transaction(fm, lfm, bm);
//    }
//
//    public MetaDataManager getMdm(){
//        return mdm;
//    }
//
//    public ExecutorManager getEm(){
//        return em;
//    }
//
//    public FileManager getFm(){
//        return fm;
//    }
//
//    public LogFileManager getLfm(){
//        return lfm;
//    }
//
//    public BufferManager getbm(){
//        return bm;
//    }
//}
