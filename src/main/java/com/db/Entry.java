package com.db;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;

public class Entry implements Serializable {
    private Hashtable<String, Object> htblTuple;

    public Entry(Hashtable<String, Object> htblColNameValue){
        for(String strColName: htblColNameValue.keySet()){
            Object oValue = htblTuple.get(strColName);
            htblTuple.put(strColName, oValue);
        }
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

    public void setHtblTuple(Hashtable<String, Object> htblColNameValue) {
        for(String strColName: htblColNameValue.keySet()){
            Object oValue = htblTuple.get(strColName);
            htblTuple.put(strColName, oValue);
        }
    }
}
