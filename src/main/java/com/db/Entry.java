package com.db;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;

public class Entry implements Serializable, Comparable<Entry>{
    private Hashtable<String, Object> htblTuple;
    private String strClusteringKey;

    public Entry(Hashtable<String, Object> htblColNameValue, String strClusteringKey){
        htblTuple = new Hashtable<>();
        for(String strColName: htblColNameValue.keySet()) {
            Object oValue = htblColNameValue.get(strColName);
            htblTuple.put(strColName, oValue);
        }
        this.strClusteringKey = strClusteringKey;
    }

    public Comparable fnEntryID() {
        return (Comparable) htblTuple.get(strClusteringKey);
    }

    @Override
    public String toString() {
        StringBuilder sbEntry = new StringBuilder();
        for (Map.Entry entry: htblTuple.entrySet()){
            sbEntry.append(entry.getValue().toString()).append(',');
        }
        sbEntry.deleteCharAt(sbEntry.length() - 1);
        return sbEntry.toString();
    }

    public Hashtable<String, Object> getHtblTuple() {
        return htblTuple;
    }

    public Object getColumnValue(String strColName) {
        return htblTuple.get(strColName);
    }

    public void setHtblTuple(Hashtable<String, Object> htblColNameValue) {
        for(String strColName: htblColNameValue.keySet()){
            Object oValue = htblColNameValue.get(strColName);
            htblTuple.put(strColName, oValue);
        }
    }

    @Override
    public int compareTo(Entry o) {
        return ((Comparable) this.htblTuple.get(strClusteringKey)).
                compareTo((Comparable)o.htblTuple.get(strClusteringKey));
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Entry)) return false;
        return compareTo((Entry) obj) == 0;
    }

    @Override
    public int hashCode() {
        return fnEntryID().hashCode();
    }

}