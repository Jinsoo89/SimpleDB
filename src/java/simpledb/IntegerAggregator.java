package simpledb;

import java.util.*;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    
    public class Aggregate {
        public int value;
        public int count;
        
        public Aggregate(int value, int count) {
            this.value = value;
            this.count = count;
        }
    }

    private static final long serialVersionUID = 1L;
    // fields
    private int gbfield;
    private int afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<Field, Integer> counts;
    private HashMap<Field, Integer> values;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        counts = new HashMap<>();
        values = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gf = gbfield == NO_GROUPING ? new IntField(0) : tup.getField(gbfield);
        IntField af = (IntField) tup.getField(afield);
        
        if (!counts.containsKey(gf)) {
            counts.put(gf, 1);
            values.put(gf, af.getValue());
        } else {
            counts.put(gf, counts.get(gf) + 1);
            int v = values.get(gf);
            int av = af.getValue();
            
            switch (what) {
            case AVG:
            case COUNT:
            case SUM:
                values.put(gf, v + av);
                break;
            case MIN:
                values.put(gf, Math.min(v, av));
                break;
            case MAX:
                values.put(gf, Math.max(v, av));
                break;
            default:
                break;
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        boolean isGroup = gbfield != NO_GROUPING;
        
        if (isGroup) {
            td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        } else {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
        }
        
        for (Field gf : counts.keySet()) {
            Tuple tuple = new Tuple(td);
            
            if (isGroup) {
                tuple.setField(0, gf);
            }
            
            tuple.setField(isGroup ? 1 : 0, new IntField(takeValueOut(gf)[0]));
                     
            tuples.add(tuple);
        }
        
        return new TupleIterator(td, tuples);
    }
    
    // helper function to take out the aggregate value
    public Integer[] takeValueOut(Field gf) {
        switch (what) {
        case COUNT:
            return new Integer[] { counts.get(gf) };
        case SUM:
            return new Integer[] { values.get(gf) };
        case AVG:
            return new Integer[] { values.get(gf) / counts.get(gf) };
        case MIN:
            return new Integer[] { values.get(gf) };
        case MAX:
            return new Integer[] { values.get(gf) };
        default:
            break;
        }
        return new Integer[] { };
    }

}
