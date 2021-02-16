package server;

import java.io.File;

import buffer.BufferManager;
import file.FileManager;
import log.LogFileManager;
import metadata.mgr.MetaMgr;
import mvcc.Transaction;
import plan.Impl.Planner;
import plan.Impl.UpdatePlanner;
import plan.Impl.QueryPlanner;

public class CosmicDB {
    public static int BLOCK_SIZE = 400;
    public static int BUFFER_SIZE = 8;
    public static String LOGFILE = "CosmicDB.log";

    private FileManager fm;
    private LogFileManager lfm;
    private BufferManager bm;

    private MetaMgr mm;
    private Planner em;
    private Transaction tx;

    /**
     * Constructor Function
     * @param dirname
     * @param blocksize
     * @param bufsize
     */
    public CosmicDB(String dirname, int blocksize, int bufsize){
        File dbDirectory = new File(dirname);
        fm = new FileManager(dbDirectory, blocksize);
        lfm = new LogFileManager(fm, LOGFILE);
        bm = new BufferManager(fm, lfm, bufsize);
    }

    public CosmicDB(String dirname){
        this(dirname, BLOCK_SIZE, BUFFER_SIZE);
        //创建事务 事务语句原子执行，不可被中断，目前单线程，所以不考虑这个
        //Transaction tx = new newTx();
        boolean newdb = fm.isNewdb();
        if(newdb){
            System.out.println("Create New DataBase Successful");
        }else{
            System.out.println("Recover Existed DataBase");
            tx.recover();
        }
        mm = new MetaMgr(newdb, tx);
        QueryPlanner qP = new QueryPlanner(mm);
        UpdatePlanner uP = new UpdatePlanner(mm);

        em = new Planner(qP, uP);
        // tx.commit();
    }

    public Transaction newTx(){
        return new Transaction(fm, lfm, bm);
    }

    public MetaMgr getMm(){
        return mm;
    }

    public Planner getEm(){
        return em;
    }

    public FileManager getFm(){
        return fm;
    }

    public LogFileManager getLfm(){
        return lfm;
    }

    public BufferManager getbm(){
        return bm;
    }
}
