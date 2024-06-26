package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator aggregatorIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.aggregator = null;
        this.aggregatorIterator = null;

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        if (gfield != Aggregator.NO_GROUPING) {
            return child.getTupleDesc().getFieldName(this.gfield);
        }
        return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {

        if (this.aggregatorIterator == null) {
            this.aggregator = this.createAggregator(this.child.getTupleDesc());
            this.populateAggregator();
            this.aggregatorIterator = this.aggregator.iterator();
        }
        this.aggregatorIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggregatorIterator.hasNext()) {
            return this.aggregatorIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.aggregatorIterator = this.aggregator.iterator();
        this.aggregatorIterator.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        String aggFieldName = Aggregate.aggName(this.aop) + " (" +
                this.child.getTupleDesc().getFieldName(this.afield) + ")";

        if (this.gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(
                    new Type[] { this.child.getTupleDesc().getFieldType(this.afield) },
                    new String[] { aggFieldName });
        } else {
            return new TupleDesc(
                    new Type[] {
                            this.child.getTupleDesc().getFieldType(this.gfield),
                            this.child.getTupleDesc().getFieldType(this.afield)
                    },
                    new String[] {
                            this.child.getTupleDesc().getFieldName(this.gfield),
                            aggFieldName
                    });
        }
    }

    public void close() {
        super.close();
        this.child.close();
        this.aggregatorIterator.close();
        this.aggregatorIterator = null;
        this.aggregator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children[0] != this.child) {
            this.child = children[0];
        }
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
            case MIN:
                return "min";
            case MAX:
                return "max";
            case AVG:
                return "avg";
            case SUM:
                return "sum";
            case COUNT:
                return "count";
            default:
                return "";
        }
    }

    private Aggregator createAggregator(TupleDesc td) throws DbException {
        Type groupByFieldType = null;
        if (this.gfield != Aggregator.NO_GROUPING) {
            groupByFieldType = td.getFieldType(this.gfield);
        }

        if (td.getFieldType(this.afield) == Type.INT_TYPE) {
            return new IntegerAggregator(this.gfield, groupByFieldType, this.afield, this.aop);
        } else if (td.getFieldType(this.afield) == Type.STRING_TYPE) {
            return new StringAggregator(this.gfield, groupByFieldType, this.afield, this.aop);
        } else {
            throw new DbException("This type of iterator is not supported");
        }
    }

    private void populateAggregator() throws DbException, TransactionAbortedException {
        this.child.open();

        while (this.child.hasNext()) {
            Tuple nextTuple = this.child.next();
            this.aggregator.mergeTupleIntoGroup(nextTuple);
        }
    }

}
