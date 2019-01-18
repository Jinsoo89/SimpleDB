package simpledb;

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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        List<TDItem> temp = new ArrayList<>(Arrays.asList(items));

        return temp.iterator();
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] items;

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
        if (typeAr.length == 0 || typeAr.length != fieldAr.length) {
            throw new RuntimeException();
        }
        int size = typeAr.length;

        items = new TDItem[size];

        for (int i = 0; i < size; i++) {
            TDItem item = new TDItem(typeAr[i], fieldAr[i]);
            items[i] = item;
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
        if (typeAr.length == 0) {
            throw new RuntimeException();
        }
        int size = typeAr.length;

        items = new TDItem[size];

        for (int i = 0; i < size; i++) {
            TDItem item = new TDItem(typeAr[i], null);
            items[i] = item;
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.length;
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
        // i is not valid
        if (i > numFields() || i < 0) {
            throw new NoSuchElementException();
        }

        return items[i].fieldName;
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
        // i is not valid
        if (i > numFields() || i < 0) {
            throw new NoSuchElementException();
        }

        return items[i].fieldType;
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
        for (int i = 0; i < numFields(); i++) {
            if (Objects.equals(getFieldName(i), name)) {
                return i;
            }
        }
        // no matching name is found
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;

        for (int i = 0; i < items.length; i++) {
            totalSize += items[i].fieldType.getLen();
        }

        return totalSize;
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
        int mergeSize = td1.numFields() + td2.numFields();
        Type[] typeArr = new Type[mergeSize];
        String[] fieldArr = new String[mergeSize];

        // coping elements of two TupleDesc to merge
        for (int i = 0; i < td1.numFields(); i++) {
            typeArr[i] = td1.getFieldType(i);
            fieldArr[i] = td1.getFieldName(i);
        }
        for (int j = td1.numFields(); j < mergeSize; j++) {
            typeArr[j] = td2.getFieldType(j - td1.numFields());
            fieldArr[j] = td2.getFieldName(j - td1.numFields());
        }

        return new TupleDesc(typeArr, fieldArr);
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
        if (o instanceof TupleDesc) {
            TupleDesc temp = (TupleDesc) o;

            if (this.numFields() != temp.numFields()) {
                return false;
            }

            for (int i = 0; i < this.numFields(); i++) {
                if (!this.getFieldType(i).equals(temp.getFieldType(i))) {
                    return false;
                }
            }
            // two objects are equal
            return true;
        } else {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String str = "";

        for (int i = 0; i < numFields(); i++) {
            str += getFieldType(i) + "(" + getFieldName(i) + "), ";
        }

        return str;
    }
}
