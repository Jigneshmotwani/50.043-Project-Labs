package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    private ArrayList<TDItem> tdItems;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0) {
            throw new IllegalArgumentException("Length of typeAr must be at least 1");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("Length of typeAr and fieldAr must be the same");
        }
        this.tdItems = new ArrayList<>();
        for (int i = 0; i < typeAr.length; i++) {
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            this.tdItems.add(tdItem);
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this.tdItems = new ArrayList<>();

        for (int i = 0; i < typeAr.length; i++) {
            TDItem tdItem = new TDItem(typeAr[i], "");
            this.tdItems.add(tdItem);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > this.numFields() - 1) {
            throw new NoSuchElementException("Invalid field reference");
        }
        else {
            return this.tdItems.get(i).fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i > this.numFields() - 1) {
            throw new NoSuchElementException();
        }
        else {
            return this.tdItems.get(i).fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (TDItem item: this.tdItems) {
            int fieldIndex = this.tdItems.indexOf(item);
            if (item.fieldName != null && item.fieldName.equals(name)) {
                return fieldIndex;
            }
            else if (item.fieldName == null && name == null) {
                return fieldIndex;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item: this.tdItems) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        ArrayList<TDItem> newTDItems = new ArrayList<>();
        for (TDItem item: td1.tdItems) {
            newTDItems.add(item);
        }
        for (TDItem item: td2.tdItems) {
            newTDItems.add(item);
        }
        Type[] typeAr = new Type[newTDItems.size()];
        String[] fieldAr = new String[newTDItems.size()];
        for (int i = 0; i < newTDItems.size(); i++) {
            typeAr[i] = newTDItems.get(i).fieldType;
            fieldAr[i] = newTDItems.get(i).fieldName;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        // Check whether it's a valid object
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc obj = (TupleDesc) o;
        if (this.numFields() != obj.numFields()) {
            return false;
        }

        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(obj.getFieldType(i))) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // return this.toString().hashCode();
        return 0;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < this.numFields(); i++) {
            sb.append(
                this.getFieldType(i).toString() + "(" + this.getFieldName(i) + "), "
            );
        }
        // Remove trailing characters
        // get substring of SB
        return sb.substring(0, sb.length() - 2);
    }
}
