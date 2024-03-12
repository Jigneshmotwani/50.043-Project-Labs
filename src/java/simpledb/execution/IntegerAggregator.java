package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;
import simpledb.storage.IntField;
import simpledb.storage.Field;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> aggregateValues;
    private HashMap<Field, Integer> countValues;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *                    the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *                    the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null
     *                    if there is no grouping
     * @param afield
     *                    the 0-based index of the aggregate field in the tuple
     * @param what
     *                    the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregateValues = new HashMap<>();
        this.countValues = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = this.getGroupByField(tup);
        int updatedCount = this.getUpdatedCount(tup);
        this.countValues.put(groupByField, updatedCount);
        int updatedAggregate = this.getUpdatedAggregate(tup);
        this.aggregateValues.put(groupByField, updatedAggregate);

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
        TupleDesc groupAggregateTd;
        ArrayList<Tuple> tuples = new ArrayList<>();
        boolean hasGrouping = (this.gbfield != Aggregator.NO_GROUPING);

        if (hasGrouping) {
            groupAggregateTd = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
        } else {
            groupAggregateTd = new TupleDesc(new Type[] { Type.INT_TYPE });
        }

        for (Map.Entry<Field, Integer> groupAggregateEntry : this.aggregateValues.entrySet()) {
            Tuple groupAggregateTuple = new Tuple(groupAggregateTd);
            Integer finalAggregateValue = this.getFinalAggregateValue(
                    countValues.get(groupAggregateEntry.getKey()),
                    groupAggregateEntry.getValue());

            // If there is a grouping, we return a tuple in the form {groupByField,
            // aggregateVal}
            // If there is no grouping, we return a tuple in the form {aggregateVal}
            if (hasGrouping) {
                groupAggregateTuple.setField(0, groupAggregateEntry.getKey());
                groupAggregateTuple.setField(1, new IntField(finalAggregateValue));
            } else {
                groupAggregateTuple.setField(0, new IntField(finalAggregateValue));
            }
            tuples.add(groupAggregateTuple);
        }
        return new TupleIterator(groupAggregateTd, tuples);
    }

    private int getUpdatedCount(Tuple tup) {
        Field groupByField = getGroupByField(tup);
        int currentCount = this.countValues.getOrDefault(groupByField, 0);
        return ++currentCount;
    }

    private int getUpdatedAggregate(Tuple tup) {
        IntField tupleAggregate = (IntField) tup.getField((this.afield));
        Field groupByField = getGroupByField(tup);
        Integer defaultValue = (this.what == Op.MIN || this.what == Op.MAX) ? tupleAggregate.getValue() : 0;
        Integer currentAggregateValue = this.aggregateValues.getOrDefault(groupByField, defaultValue);
        Integer updatedAggregateValue = currentAggregateValue;

        switch (this.what) {
            case AVG:
                updatedAggregateValue = currentAggregateValue + tupleAggregate.getValue();
                break;
            case MAX:
                updatedAggregateValue = Math.max(currentAggregateValue, tupleAggregate.getValue());
                break;
            case MIN:
                updatedAggregateValue = Math.min(currentAggregateValue, tupleAggregate.getValue());
                break;
            case SUM:
                updatedAggregateValue = currentAggregateValue + tupleAggregate.getValue();
                break;
            case COUNT:
                // This case is handled by groupCount
                break;
            default:
                break;
        }
        return updatedAggregateValue;
    }

    private Field getGroupByField(Tuple tup) {
        Field groupByField;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            groupByField = null;
        } else {
            groupByField = tup.getField(this.gbfield);
        }
        return groupByField;
    }

    private Integer getFinalAggregateValue(Integer totalCount, Integer aggregateValue) {
        Integer finalAggregateValue;
        switch (this.what) {
            case AVG:
                finalAggregateValue = aggregateValue / totalCount;
                break;
            case COUNT:
                finalAggregateValue = totalCount;
                break;
            default:
                finalAggregateValue = aggregateValue;
                break;
        }
        return finalAggregateValue;
    }

}
