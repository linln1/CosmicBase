package plan.Impl;

import excutor.Scan;
import excutor.UpdateScan;
import excutor.impl.SortScan;
import mvcc.Transaction;
import plan.Plan;
import record.RecordComparator;
import record.Schema;
import record.TempTable;

import java.util.ArrayList;
import java.util.List;


/**
 * sort operator
 */
public class SortPlan implements Plan{
    private Transaction tx;
    private Plan p;
    private Schema sch;
    private RecordComparator comp;


    public SortPlan(Transaction tx, Plan p, List<String> sortfields) {
        this.tx = tx;
        this.p = p;
        this.sch = p.schema();
        this.comp = new RecordComparator(sortfields);
    }

    @Override
    public Scan open() {
        Scan src = (Scan) p.open();
        List<TempTable> runs = splitIntoRuns(src);
        src.close();
        while (runs.size() > 2 ){
            runs = doAMergeIteration(runs);
        }
        return new SortScan(runs, comp);
    }

    private List<TempTable> doAMergeIteration(List<TempTable> runs) {
        List<TempTable> result = new ArrayList<>();
        while(runs.size() > 1){
            TempTable p1 = runs.remove(0);
            TempTable p2 = runs.remove(0);
            result.add(mergeTwoRuns(p1, p2));
        }
        if(runs.size() == 1){
            result.add(runs.get(0));
        }
        return result;
    }

    private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
        Scan src1 = p1.open();
        Scan src2 = p2.open();
        TempTable result = new TempTable(tx, sch);
        UpdateScan dest = result.open();

        boolean hasmore1 = src1.nextPtr();
        boolean hasmore2 = src2.nextPtr();
        while (hasmore1 && hasmore2) {
            if (comp.compare(src1, src2) < 0) {
                hasmore1 = copy(src1, dest);
            } else {
                hasmore2 = copy(src2, dest);
            }
        }

        if (hasmore1) {
            while (hasmore1) {
                hasmore1 = copy(src1, dest);
            }
        } else {
            while (hasmore2) {
                hasmore2 = copy(src2, dest);
            }
        }
        src1.close();
        src2.close();
        dest.close();
        return result;
    }

    private boolean copy(Scan src1, UpdateScan dest) {
        dest.insert();
        for(String fldname : sch.fields()){
            dest.setConstant(fldname, src1.getVal(fldname));
        }
        return src1.nextPtr();
    }

    private List<TempTable> splitIntoRuns(Scan src) {
        List<TempTable> temps = new ArrayList<>();
        src.StartPtr();
        if (!src.nextPtr())
            return temps;
        TempTable currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        UpdateScan currentscan = currenttemp.open();
        while (copy(src, currentscan)) {
            if (comp.compare(src, currentscan) < 0) {
                // start a new run
                currentscan.close();
                currenttemp = new TempTable(tx, sch);
                temps.add(currenttemp);
                currentscan = (UpdateScan) currenttemp.open();
            }
        }
        currentscan.close();
        return temps;
    }

    @Override
    public int blockAccessed() {
        Plan mp = new MaterializePlan(tx, p);
        return mp.blockAccessed();
    }

    /**
     * Return the number of records in the sorted table,
     * which is the same as in the underlying query
     * @return
     */
    @Override
    public int recordsOutput() {
        return p.recordsOutput();
    }

    @Override
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return sch;
    }
}
