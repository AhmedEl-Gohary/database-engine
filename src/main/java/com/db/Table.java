package com.db;

import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
    String strTableName;
    String strClusteringKeyColumn;
    Vector<String> vecPages;
    Vector<Comparable> vecMin;
    Vector<Integer> vecCountRows;

    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
        vecPages = new Vector<>();
        vecMin = new Vector<>();
        vecCountRows = new Vector<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
    }

    public void fnInsertNewPage(Hashtable<String, Object> htblColNameValue){
        int iPageNumber = vecPages.size();
        vecPages.add(this.strTableName + vecPages.size());
        vecMin.add((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Page pageInstance = new Page(this.strTableName, iPageNumber);
        DBApp.fnSerialize(pageInstance, strTableName + iPageNumber);
        vecCountRows.add(0);
    }

    public void fnInsertEntry(Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (htblColNameValue.get(this.strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        int iPageNumber = fnBSPageLocation((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Entry entryInstance = new Entry(htblColNameValue, this.strClusteringKeyColumn);
        while(entryInstance != null) {
            if (iPageNumber == vecPages.size()) {
                fnInsertNewPage(htblColNameValue);
            }
            Page pageInstance = (Page) DBApp.fnDeserialize(strTableName + iPageNumber);
            entryInstance = pageInstance.fnInsertEntry(entryInstance);
            vecMin.set(iPageNumber, pageInstance.vecTuples.firstElement().fnEntryID());
            DBApp.fnSerialize(pageInstance, strTableName + iPageNumber);
            iPageNumber++;
        }
        vecCountRows.set(iPageNumber - 1, vecCountRows.get(iPageNumber - 1) + 1);
    }

    public void fnDeleteEntry(Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (htblColNameValue.get(this.strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        int iPageNumber = fnBSPageLocation((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Page pageBlock = (Page) DBApp.fnDeserialize(vecPages.get(iPageNumber));
        Entry entryTuple = new Entry(htblColNameValue, strClusteringKeyColumn);
        int iEntryIdx = Collections.binarySearch(pageBlock.vecTuples, entryTuple);
        if (iEntryIdx >= 0){
            pageBlock.vecTuples.remove(iEntryIdx);

        }
        //TODO: delete empty pages
    }

    public int fnCountPages(){
        return vecPages.size();
    }

    private boolean fnIsFull(int iPageIdx){
        return vecCountRows.get(iPageIdx) == Page.iMaxRowsCount;
    }
    public int fnBSPageLocation(Comparable oTarget){
        int N = vecPages.size();
        int l = 0, r = N - 1;
        int iFirstGoodIdx = 0;
        while (l <= r) {
            int mid = l + r >> 1;
            if (oTarget.compareTo(vecMin.get(mid)) >= 0) {
                iFirstGoodIdx = mid;
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        return iFirstGoodIdx;
    }


    public static void main(String[] args) throws DBAppException {
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Table t = new Table(strTableName, "id", htblColNameType);

    }

}