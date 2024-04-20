package com.db;

import java.io.Serializable;
import java.util.Collections;
import java.util.Vector;

/**
 * Represents a page in the database.
 */

public class Page implements Serializable {
    /** The name of the table to which the page belongs. */
    String strTableName;
    /** The page number. */
    int iPageNumber;
    /** The maximum number of rows allowed on the page. */
    public static int iMaxRowsCount;
    /** The vector storing entries (tuples) on the page. */
    Vector<Entry> vecTuples;

    /**
     * Constructs a new Page object for the specified table and page number.
     *
     * @param strTableName The name of the table.
     * @param iPageNumber The page number.
     */
    public Page(String strTableName, int iPageNumber){
        this.strTableName = strTableName;
        this.iPageNumber = iPageNumber;
        vecTuples = new Vector<>();
    }

    /**
     * Returns a string representation of the page.
     *
     * @return A string representing the page.
     */
    @Override
    public String toString() {
        StringBuilder sbPage = new StringBuilder();
        for (Entry entryTuple: vecTuples){
            sbPage.append(entryTuple).append('\n');
        }
        return sbPage.toString();
    }

    /**
     * Checks if the page is full.
     *
     * @return true if the page is full; false otherwise.
     */
    public boolean fnIsFull(){
        return vecTuples.size() == iMaxRowsCount;
    }

    /**
     * Checks if the page is overflowing.
     *
     * @return true if the page is overflowing; false otherwise.
     */
    public boolean fnIsOverFlow(){
        return vecTuples.size() > iMaxRowsCount;
    }

    /**
     * Checks if the page is empty.
     *
     * @return true if the page is empty; false otherwise.
     */
    public boolean fnIsEmpty(){
        return vecTuples.isEmpty();
    }

    /**
     * Retrieves the name of the page.
     *
     * @return The name of the page.
     */
    public String fnGetPageName(){
        return strTableName + iPageNumber;
    }

    /**
     * Inserts an entry into the page.
     *
     * @param entryInstance The entry to insert.
     * @return The entry instance if overflow occurs; null otherwise.
     * @throws DBAppException if attempting to insert duplicates.
     */
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