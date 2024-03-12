package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> countMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.countMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupField = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        countMap.putIfAbsent(groupField, 0);
        countMap.put(groupField, countMap.get(groupField) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;

        if (this.gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
        } else {
            td = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
        }

        for (Map.Entry<Field, Integer> entry : countMap.entrySet()) {
            Tuple tuple = new Tuple(td);
            Field groupField = entry.getKey();
            Integer count = entry.getValue();

            if (this.gbfield == NO_GROUPING) {
                tuple.setField(0, new IntField(count));
            } else {
                tuple.setField(0, groupField);
                tuple.setField(1, new IntField(count));
            }

            tuples.add(tuple);
        }

        return new TupleIterator(td, tuples);
    }

}
