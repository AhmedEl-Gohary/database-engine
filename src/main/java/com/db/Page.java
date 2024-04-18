package com.db;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.Properties;
import java.util.Vector;
import java.io.*;

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

    public boolean fnIsFull(){
        return vecTuples.size() == iMaxRowsCount;
    }

    public boolean fnIsOverFlow(){
        return vecTuples.size() > iMaxRowsCount;
    }

    public boolean fnIsEmpty(){
        return vecTuples.isEmpty();
    }

    public String fnGetPageName(){
        return strTableName + iPageNumber;
    }

    public Entry fnInsertEntry(Entry entryInstance) throws DBAppException {
        int index = Collections.binarySearch(vecTuples, entryInstance);

        if (index < 0) {
            int iInsertionPoint = -index - 1;
            vecTuples.add(iInsertionPoint,entryInstance);
            if(this.fnIsOverFlow()){
                entryInstance = vecTuples.lastElement();
                vecTuples.remove(vecTuples.size()-1);
                return entryInstance;
            }
            return null;
        }
        else {
            throw new DBAppException("Cannot insert duplicates");
        }
    }
}