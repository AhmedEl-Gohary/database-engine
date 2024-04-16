package com.db;

import java.io.Serializable;
import java.util.Properties;
import java.util.Vector;

public class Page implements Serializable {
    String strTableName;
    int iPageNumber;
    public static int iMaxRowsCount;
    Vector<Entry> vecTuples;
    public Page(String strTableName, int iPageNumber){
        this.strTableName = strTableName;
        this.iPageNumber = iPageNumber;
        vecTuples = new Vector<>();
    }

    @Override
    public String toString() {
        StringBuilder sbPage = new StringBuilder();
        for (Entry entryTuple: vecTuples){
            sbPage.append(entryTuple).append('\n');
        }
        return sbPage.toString();
    }
}
