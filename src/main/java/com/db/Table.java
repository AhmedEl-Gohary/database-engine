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
    int iCreatedPages ;

    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
        vecPages = new Vector<>();
        vecMin = new Vector<>();
        vecCountRows = new Vector<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.iCreatedPages = 0;
    }

    public void fnInsertNewPage(Hashtable<String, Object> htblColNameValue){
        iCreatedPages++;
        vecPages.add(this.strTableName + iCreatedPages);
        vecMin.add((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Page pageInstance = new Page(this.strTableName, iCreatedPages);
        DBApp.fnSerialize(pageInstance, strTableName + iCreatedPages);
        vecCountRows.add(0);
    }

    public String fnInsertEntry(Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (htblColNameValue.get(this.strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        int iPageNumber = fnBSPageLocation((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Entry entryInstance = new Entry(htblColNameValue, this.strClusteringKeyColumn);
        while(entryInstance != null) {
            if (iPageNumber == vecPages.size()) {
                fnInsertNewPage(htblColNameValue);
            }
            Page pageInstance = (Page) DBApp.fnDeserialize(vecPages.get(iPageNumber));
            entryInstance = pageInstance.fnInsertEntry(entryInstance);
            vecMin.set(iPageNumber, pageInstance.vecTuples.firstElement().fnEntryID());
            DBApp.fnSerialize(pageInstance, vecPages.get(iPageNumber));
            iPageNumber++;
        }
        --iPageNumber;
        vecCountRows.set(iPageNumber , vecCountRows.get(iPageNumber) + 1);
        return vecPages.get(iPageNumber);
    }
    public Entry fnSearchEntryWithClusteringKey(Hashtable<String,Object> htblColNameValue,String strClusteringKeyColumn) throws DBAppException {
        if (htblColNameValue.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        Comparable cmpClusteringKey = (Comparable) htblColNameValue.get(strClusteringKeyColumn);
        int iPageNumber = fnBSPageLocation(cmpClusteringKey);
        Page pageBlock = (Page) DBApp.fnDeserialize(vecPages.get(iPageNumber));
        Entry entryTuple = new Entry(htblColNameValue, strClusteringKeyColumn);
        int iEntryIdx = Collections.binarySearch(pageBlock.vecTuples, entryTuple);
        if (iEntryIdx >= 0){
            return pageBlock.vecTuples.get(iEntryIdx);
        }
        return null;
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
    public void fnDeleteEntry(Entry entry){
        int iPageNumber = fnBSPageLocation(entry.fnEntryID());
        Page pageInstance = (Page) DBApp.fnDeserialize(vecPages.get(iPageNumber));
        int iEntryIdx = Collections.binarySearch(pageInstance.vecTuples, entry);
        if (iEntryIdx >= 0){
            pageInstance.vecTuples.remove(iEntryIdx);
        }

        if(pageInstance.vecTuples.isEmpty()){
            DBApp.fnDeleteFile(vecPages.get(iPageNumber));
            vecPages.remove(iPageNumber);
            vecMin.remove(iPageNumber);
            vecCountRows.remove(iPageNumber);
        }
        else{
            vecMin.set(iPageNumber, pageInstance.vecTuples.firstElement().fnEntryID());
            vecCountRows.set(iPageNumber , vecCountRows.get(iPageNumber ) - 1);
            DBApp.fnSerialize(pageInstance, vecPages.get(iPageNumber));
        }
    }

    public String fnGetKeyPage(Comparable oTarget){
        if (isEmpty()) return null;
        int iPageNumber = fnBSPageLocation(oTarget);
        return vecPages.get(iPageNumber);
    }

    public void fnUpdateEntry(Hashtable<String, Object> htblEntryKey, Hashtable<String, Object> htblColNameValue){
        if(this.isEmpty()){
            return;
        }
        int iPageNumber = fnBSPageLocation((Comparable) htblEntryKey.get(this.strClusteringKeyColumn));
        Page pageInstance = (Page) DBApp.fnDeserialize(vecPages.get(iPageNumber));
        Entry entrySearch = new Entry(htblEntryKey,this.strClusteringKeyColumn);
        int iEntryIdx = Collections.binarySearch(pageInstance.vecTuples, entrySearch );
        if(iEntryIdx >=0){
            Entry entryFetch =  pageInstance.vecTuples.get(iEntryIdx);
            entryFetch.setHtblTuple(htblColNameValue);


        }
        DBApp.fnSerialize(pageInstance, vecPages.get(iPageNumber));
    }


    public boolean isEmpty(){
        return vecMin.isEmpty();
    }



    public static void main(String[] args) throws DBAppException {
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Table t = new Table(strTableName, "id", htblColNameType);

    }
    @Override
    public String toString(){
        StringBuilder st = new StringBuilder();
        for(String page: vecPages){
            Page pageInstance = (Page) DBApp.fnDeserialize(page);
            st.append(pageInstance).append('\n');
        }
        return st.toString();
    }

}