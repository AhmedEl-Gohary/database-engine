package com.db;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

/**
 * Represents an entry in a database table.
 */

public class Entry implements Serializable, Comparable<Entry>{
    /** The tuple storing column name-value pairs. */
    private Hashtable<String, Object> htblTuple;
    /** The clustering key associated with the entry. */
    private String strClusteringKey;

    /**
     * Constructs a new Entry object with the given column name-value pairs and clustering key.
     *
     * @param htblColNameValue The column name-value pairs.
     * @param strClusteringKey The clustering key.
     */


    public Entry(Hashtable<String, Object> htblColNameValue, String strClusteringKey){
        htblTuple = new Hashtable<>();
        for(String strColName: htblColNameValue.keySet()) {
            Object oValue = htblColNameValue.get(strColName);
            htblTuple.put(strColName, oValue);
        }
        this.strClusteringKey = strClusteringKey;
    }
    /**
     * Retrieves the ID of the entry based on its clustering key.
     *
     * @return The clustering key value.
     */

    public Comparable getClusteringKeyValue() {
        return (Comparable) htblTuple.get(strClusteringKey);
    }
    /**
     * Returns a string representation of the entry.
     *
     * @return A string representing the entry.
     */

    @Override
    public String toString() {
        StringBuilder sbEntry = new StringBuilder();
        for (Map.Entry entry: htblTuple.entrySet()){
            sbEntry.append(entry.getValue().toString()).append(',');
        }
        sbEntry.deleteCharAt(sbEntry.length() - 1);
        return sbEntry.toString();
    }
    /**
     * Retrieves the tuple storing column name-value pairs.
     *
     * @return The tuple.
     */

    public Hashtable<String, Object> getHtblTuple() {
        return htblTuple;
    }

    /**
     * Retrieves the value of a specific column in the entry.
     *
     * @param strColName The name of the column.
     * @return The value of the column.
     */

    public Object getColumnValue(String strColName) {
        return htblTuple.get(strColName);
    }
    /**
     * Sets the tuple storing column name-value pairs.
     *
     * @param htblColNameValue The new tuple.
     */

    public void setHtblTuple(Hashtable<String, Object> htblColNameValue) {
        for(String strColName: htblColNameValue.keySet()){
            Object oValue = htblColNameValue.get(strColName);
            htblTuple.put(strColName, oValue);
        }
    }
    /**
     * Compares this entry with another entry for ordering.
     *
     * @param o The entry to be compared.
     * @return A negative integer, zero, or a positive integer as this entry is less than, equal to, or greater than the specified entry.
     */

    @Override
    public int compareTo(Entry o) {
        return ((Comparable) this.htblTuple.get(strClusteringKey)).
                compareTo((Comparable)o.htblTuple.get(strClusteringKey));
    }
    /**
     * Indicates whether some other object is "equal to" this one.
     *
     * @param obj The reference object with which to compare.
     * @return true if this object is the same as the obj argument; false otherwise.
     */

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Entry)) return false;
        return compareTo((Entry) obj) == 0;
    }
    /**
     * Checks if the provided column name-value pairs are equal to the ones stored in this entry.
     *
     * @param htblColNameValue The column name-value pairs to compare.
     * @return true if the provided column name-value pairs are equal to the ones stored in this entry; false otherwise.
     */
    public boolean equals(Hashtable<String, Object> htblColNameValue) {
        for(String strColName:htblColNameValue.keySet()) {
            if ((htblColNameValue.containsKey(strColName)) && !getColumnValue(strColName).equals(htblColNameValue.get(strColName))) {
                return false;
            }
        }
        return true;
    }
    /**
     * Returns a hash code value for the entry.
     *
     * @return A hash code value for this entry.
     */

    @Override
    public int hashCode() {
        return getClusteringKeyValue().hashCode();
    }

}