package simpledb;

import simpledb.Aggregator.Op;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    // fields
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<Field, Integer> counts;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
//        if (gbfield != NO_GROUPING) {
//            String gfname = tup.getTupleDesc().getFieldName(gbfield);
            Field gf = tup.getField(gbfield);
            Integer c = counts.get(gf);
            counts.put(gf, c == null ? 1 : c + 1);
            
//            if (!counts.containsKey(gf)) {
//                counts.put(gf, 1);
//            } else {
//                int c = counts.get(gf);
//                counts.put(gf, c + 1);
//            }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        
        if (gbfield != NO_GROUPING) {
            td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
            
            for (Field f : counts.keySet()) {
                Tuple tuple = new Tuple(td);
                
                tuple.setField(0, f);
                tuple.setField(1, new IntField(counts.get(f)));
                
                tuples.add(tuple);
            }
        } else {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
            
            Tuple tuple = new Tuple(td);
            
            tuple.setField(0, new IntField(counts.get(null)));
            
            tuples.add(tuple);
        }
        return new TupleIterator(td, tuples);
    }

}
