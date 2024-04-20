/**
 * Represents a table in the database.
 */
package com.db;

import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
    /** The name of the table. */
    String strTableName;
    /** The name of the clustering key column. */
    String strClusteringKeyColumn;
    /** Vector storing the names of pages belonging to the table. */
    Vector<String> vecPages;
    /** Vector storing the minimum values of each page's clustering key. */
    Vector<Comparable> vecMin;
    /** Vector storing the count of rows in each page. */
    Vector<Integer> vecCountRows;
    /** The number of created pages. */
    int iCreatedPages ;

    /**
     * Constructs a new Table object.
     *
     * @param strTableName The name of the table.
     * @param strClusteringKeyColumn The name of the clustering key column.
     * @param htblColNameType The hashtable mapping column names to their types.
     * @throws DBAppException if an error occurs during table creation.
     */
    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
        vecPages = new Vector<>();
        vecMin = new Vector<>();
        vecCountRows = new Vector<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        this.iCreatedPages = 0;
    }
    /**
     * Inserts a new page into the table.
     *
     * @param htblColNameValue The hashtable containing column names and values.
     */
    public void fnInsertNewPage(Hashtable<String, Object> htblColNameValue){
        iCreatedPages++;
        vecPages.add(this.strTableName + iCreatedPages);
        vecMin.add((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Page pageInstance = new Page(this.strTableName, iCreatedPages);
        DBApp.serialize(pageInstance, strTableName + iCreatedPages);
        vecCountRows.add(0);
    }
    /**
     * Inserts an entry into the table.
     *
     * @param htblColNameValue The hashtable containing column names and values of the entry.
     * @throws DBAppException if an error occurs during insertion.
     */
    public void fnInsertEntry(Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (htblColNameValue.get(this.strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        int iPageNumber = fnGetPageLocation((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        if (iPageNumber == - 1) iPageNumber = 0;
        Entry entryInstance = new Entry(htblColNameValue, this.strClusteringKeyColumn);
        Hashtable<String, String> colIndicesNames = Meta.fnMapColumnToIndexName(strTableName);
        Hashtable<String, Index> colIndices = new Hashtable<>();
        for (String colName: colIndicesNames.keySet()){
            colIndices.put(colName, (Index) DBApp.deserialize(colIndicesNames.get(colName)));
        }
        while(entryInstance != null) {
            if (iPageNumber == vecPages.size()) {
                fnInsertNewPage(htblColNameValue);
            }
            Page pageInstance = (Page) DBApp.deserialize(vecPages.get(iPageNumber));
            for (String colName: colIndices.keySet()){
                Object value = entryInstance.getColumnValue(colName);
                Index index = colIndices.get(colName);
                if (value != null) {
                    index.updatePage((Comparable) value, entryInstance.fnEntryID(), pageInstance.fnGetPageName());
                }
            }
            entryInstance = pageInstance.fnInsertEntry(entryInstance);
            vecMin.set(iPageNumber, pageInstance.vecTuples.firstElement().fnEntryID());
            DBApp.serialize(pageInstance, vecPages.get(iPageNumber));
            iPageNumber++;
        }
        for (String colName: colIndices.keySet()){
            DBApp.serialize(colIndices.get(colName), colIndicesNames.get(colName));
        }
        --iPageNumber;
        vecCountRows.set(iPageNumber , vecCountRows.get(iPageNumber) + 1);
    }
    /**
     * Searches for an entry with the specified clustering key in the table.
     *
     * @param htblColNameValue The hashtable containing column names and values.
     * @param strClusteringKeyColumn The clustering key column name.
     * @return The entry matching the specified clustering key, or null if not found.
     * @throws DBAppException if an error occurs during search.
     */
    public Entry fnSearchEntryWithClusteringKey(Hashtable<String,Object> htblColNameValue,String strClusteringKeyColumn) throws DBAppException {
        if (htblColNameValue.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        Comparable cmpClusteringKey = (Comparable) htblColNameValue.get(strClusteringKeyColumn);
        int iPageNumber = fnGetPageLocation(cmpClusteringKey);
        if (iPageNumber == -1) iPageNumber = 0;
        Page pageBlock = (Page) DBApp.deserialize(vecPages.get(iPageNumber));
        Entry entryTuple = new Entry(htblColNameValue, strClusteringKeyColumn);
        int iEntryIdx = Collections.binarySearch(pageBlock.vecTuples, entryTuple);
        DBApp.serialize(pageBlock,vecPages.get(iPageNumber));
        if (iEntryIdx >= 0){
            return pageBlock.vecTuples.get(iEntryIdx);
        }
        return null;
    }
    /**
     * Searches for an entry with the specified clustering key in the specified page.
     *
     * @param strPageName The name of the page to search in.
     * @param htblColNameValue The hashtable containing column names and values.
     * @return The entry matching the specified clustering key, or null if not found.
     * @throws DBAppException if an error occurs during search.
     */
    public Entry fnSearchInPageWithClusteringKey(String strPageName,Hashtable<String,Object> htblColNameValue) throws DBAppException {
        if (htblColNameValue.get(strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        Page pageBlock = (Page) DBApp.deserialize(strPageName);
        Entry entryTuple = new Entry(htblColNameValue, strClusteringKeyColumn);
        int iEntryIdx = Collections.binarySearch(pageBlock.vecTuples, entryTuple);
        DBApp.serialize(pageBlock,strPageName);
        if (iEntryIdx >= 0){
            return pageBlock.vecTuples.get(iEntryIdx);
        }
        return null;
    }
    /**
     * Searches for an entry with the specified clustering key in the specified page.
     *
     * @param pair The pair containing the page name and clustering key.
     * @return The entry matching the specified clustering key, or null if not found.
     * @throws DBAppException if an error occurs during search.
     */
    public Entry fnSearchInPageWithClusteringKey(Pair pair) throws DBAppException {
        Hashtable<String,Object> htblColNameValue = new Hashtable<>();
        htblColNameValue.put(strClusteringKeyColumn,pair.getCmpClusteringKey());
        return fnSearchInPageWithClusteringKey(pair.getStrPageName(),htblColNameValue);
    }


//    public void fnDeleteEntry(Hashtable<String,Object> htblColNameValue) throws DBAppException{
//        if (htblColNameValue.get(this.strClusteringKeyColumn) == null) {
//            throw new DBAppException("Clustering Key cannot be null!");
//        }
//        int iPageNumber = fnGetPageLocation((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
//        Page pageBlock = (Page) DBApp.fnDeserialize(vecPages.get(iPageNumber));
//        Entry entryTuple = new Entry(htblColNameValue, strClusteringKeyColumn);
//        int iEntryIdx = Collections.binarySearch(pageBlock.vecTuples, entryTuple);
//        if (iEntryIdx >= 0){
//            pageBlock.vecTuples.remove(iEntryIdx);
//
//        }
//        //TODO: delete empty pages
//    }

    /**
     * Counts the number of pages in the table.
     *
     * @return The number of pages in the table.
     */
    public int fnCountPages(){
        return vecPages.size();
    }


    /**
     * Checks if a page is full.
     *
     * @param iPageIdx The index of the page to check.
     * @return true if the page is full; false otherwise.
     */
    private boolean fnIsFull(int iPageIdx){
        return vecCountRows.get(iPageIdx) == Page.iMaxRowsCount;
    }

//    public int fnGetPageLocation(Comparable oTarget){
//        int N = vecPages.size();
//        int l = 0, r = N - 1;
//        int iFirstGoodIdx = 0;
//        while (l <= r) {
//            int mid = l + r >> 1;
//            if (oTarget.compareTo(vecMin.get(mid)) >= 0) {
//                iFirstGoodIdx = mid;
//                r = mid - 1;
//            } else {
//                l = mid + 1;
//            }
//        }
//        return iFirstGoodIdx;
//    }

    /**
     * Gets the location of a target on a page.
     *
     * @param oTarget The target object to locate.
     * @return The location index of the target.
     */
    public int fnGetPageLocation(Comparable oTarget){
        int N = vecPages.size();
        int l = 0, r = N - 1;
        int iFirstGoodIdx = -1;
        while (l <= r) {
            int mid = l + r >> 1;
            if (oTarget.compareTo(vecMin.get(mid)) >= 0) {
                iFirstGoodIdx = mid;
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        return iFirstGoodIdx;
    }

    /**
     * Deletes an entry from the table.
     *
     * @param entry The entry to delete.
     */
    public void fnDeleteEntry(Entry entry){
        int iPageNumber = fnGetPageLocation(entry.fnEntryID());
        Page pageInstance = (Page) DBApp.deserialize(vecPages.get(iPageNumber));
        int iEntryIdx = Collections.binarySearch(pageInstance.vecTuples, entry);
        if (iEntryIdx >= 0){
            pageInstance.vecTuples.remove(iEntryIdx);
        }
        if(pageInstance.vecTuples.isEmpty()){
            DBApp.deleteFile(vecPages.get(iPageNumber));
            vecPages.remove(iPageNumber);
            vecMin.remove(iPageNumber);
            vecCountRows.remove(iPageNumber);
        } else{
            vecMin.set(iPageNumber, pageInstance.vecTuples.firstElement().fnEntryID());
            vecCountRows.set(iPageNumber , vecCountRows.get(iPageNumber ) - 1);
            DBApp.serialize(pageInstance, vecPages.get(iPageNumber));
        }
    }

    /**
     * Retrieves the page name containing a specific target.
     *
     * @param oTarget The target object.
     * @return The page name containing the target, or null if not found.
     */
    public String fnGetKeyPage(Comparable oTarget){
        if (isEmpty()) return null;
        int iPageNumber = fnGetPageLocation(oTarget);
        if (iPageNumber == -1) iPageNumber = 0;
        return vecPages.get(iPageNumber);
    }
    /**
     * Updates an entry in the table.
     *
     * @param htblEntryKey The hashtable containing the entry key.
     * @param htblColNameValue The hashtable containing column names and updated values.
     * @throws DBAppException if an error occurs during update.
     */

    public void fnUpdateEntry(Hashtable<String, Object> htblEntryKey, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if(this.isEmpty()){
            return;
        }
        int iPageNumber = fnGetPageLocation((Comparable) htblEntryKey.get(this.strClusteringKeyColumn));
        if (iPageNumber == -1) return;
        Page pageInstance = (Page) DBApp.deserialize(vecPages.get(iPageNumber));
        Entry entrySearch = new Entry(htblEntryKey,this.strClusteringKeyColumn);
        int iEntryIdx = Collections.binarySearch(pageInstance.vecTuples, entrySearch );
        if(iEntryIdx >=0){
            Entry entryFetch =  pageInstance.vecTuples.get(iEntryIdx);
            this.fnUpdateTableIndecies(entryFetch ,htblColNameValue);
            entryFetch.setHtblTuple(htblColNameValue);
        }
        DBApp.serialize(pageInstance, vecPages.get(iPageNumber));
    }
    /**
     * Updates the table indices after an entry update.
     *
     * @param e The entry being updated.
     * @param htblColNameValue The hashtable containing updated column values.
     * @throws DBAppException if an error occurs during index update.
     */
    public void fnUpdateTableIndecies(Entry e ,Hashtable<String, Object> htblColNameValue) throws DBAppException {

        for(String strColName: htblColNameValue.keySet()){
            if(Meta.fnHaveColumnIndex(this.strTableName, strColName)){
                String strIndexName = Meta.fnGetColumnIndex(this.strTableName, strColName);
                Index idx = (Index) DBApp.deserialize(strIndexName);
                Object objOldKey = e.getColumnValue(strColName);
                Object objKnewValue = htblColNameValue.get(strColName);
                idx.update((Comparable) objOldKey, e.fnEntryID(), (Comparable)objKnewValue);
                DBApp.serialize(idx, strIndexName);
            }
        }
    }

    /**
     * Checks if the table is empty.
     *
     * @return true if the table is empty; false otherwise.
     */
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

    /**
     * Clears the table.
     */
    public void clear(){
        vecPages.clear();
        vecMin.clear();
        vecCountRows.clear();
        iCreatedPages = 0;
    }
    /**
     * Returns a string representation of the table.
     *
     * @return A string representing the table.
     */
    @Override
    public String toString(){
        StringBuilder st = new StringBuilder();
        for(String page: vecPages){
            Page pageInstance = (Page) DBApp.deserialize(page);
            st.append(pageInstance).append('\n');
        }
        return st.toString();
    }

}