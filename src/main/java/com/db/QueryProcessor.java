package com.db;

import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
/**
 * A utility class for processing SQL queries.
 */


public final class QueryProcessor {
    /**
     * Gets the precedence of the specified logical operator.
     *
     * @param operator The logical operator.
     * @return The precedence value.
     */
    public static int getPrecedence(String operator) {
        if (operator.equals("AND")) return 2;
        if (operator.equals("XOR")) return 1;
        return 0;
    }

    /**
     * Applies the SQL term condition to filter entries from the table.
     *
     * @param sqlTerm The SQL term representing the condition.
     * @return A vector of filtered entries.
     * @throws DBAppException if an error occurs during query processing.
     */
    public static Vector<Entry> applyCondition(SQLTerm sqlTerm) throws DBAppException {
        Table tableInstance = (Table) DBApp.deserialize(sqlTerm._strTableName);
        if (!sqlTerm._strOperator.equals("!=") && sqlTerm._strColumnName.equals(tableInstance.strClusteringKeyColumn)) {
            return clusteringQueries(sqlTerm, tableInstance);
        } else if (!sqlTerm._strOperator.equals("!=") && Meta.fnHaveColumnIndex(sqlTerm._strTableName, sqlTerm._strColumnName)) {
            Index index = (Index) DBApp.deserialize(Meta.fnGetColumnIndex(sqlTerm._strTableName, sqlTerm._strColumnName));
            return indexQueries(sqlTerm, index);
        } else {
            return linearScanning(sqlTerm, tableInstance);
        }

    }

    /**
     * Performs index-based queries based on the SQL term condition.
     *
     * @param sqlTerm The SQL term representing the condition.
     * @param index The index to query.
     * @return A vector of filtered entries.
     * @throws DBAppException if an error occurs during query processing.
     */
    public static Vector<Entry> indexQueries(SQLTerm sqlTerm, Index index) throws DBAppException {
        if (sqlTerm._strOperator.equals("=")) {
            return insertFromPagesToEntries(index.search((Comparable) sqlTerm._objValue));
        }
        if (sqlTerm._strOperator.equals(">=")) {
            return insertFromPagesToEntries(index.findGreaterThanOrEqualKey((Comparable) sqlTerm._objValue));
        }
        if (sqlTerm._strOperator.equals(">")) {
            return insertFromPagesToEntries(index.findGreaterThanKey((Comparable) sqlTerm._objValue));
        }
        if (sqlTerm._strOperator.equals("<=")) {
            return insertFromPagesToEntries(index.findLessThanOrEqualKey((Comparable) sqlTerm._objValue));
        }
        if (sqlTerm._strOperator.equals("<")) {
            return insertFromPagesToEntries(index.findLessThanKey((Comparable) sqlTerm._objValue));
        }
        throw new DBAppException("Invalid Operator!");
    }

    /**
     * Inserts entries from pages to a result set based on index search results.
     *
     * @param vecPages The vector of pages to insert from.
     * @return A vector of inserted entries.
     */
    public static Vector<Entry> insertFromPagesToEntries(Vector<Pair> vecPages) {
        Vector<Entry> resultSet = new Vector<>();
        for (Pair pair : vecPages) {
            Comparable id = pair.getCmpClusteringKey();
            String pageName = pair.getStrPageName();
            Page page = (Page) DBApp.deserialize(pageName);
            int iEntryIndex = binarySearchIndexOfEntry(page.vecTuples, id);
            resultSet.add(page.vecTuples.get(iEntryIndex));
        }
        return resultSet;
    }

    /**
     * Performs clustering queries based on the SQL term condition.
     *
     * @param sqlTerm The SQL term representing the condition.
     * @param tableInstance The table instance to query.
     * @return A vector of filtered entries.
     * @throws DBAppException if an error occurs during query processing.
     */
    public static Vector<Entry> clusteringQueries(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        if (sqlTerm._strOperator.equals("=")) {
            return binarySearchClustering(sqlTerm, tableInstance);
        }
        if (sqlTerm._strOperator.equals(">") || sqlTerm._strOperator.equals(">=")) {
            return scanFromTheEnd(sqlTerm, tableInstance);
        }
        if (sqlTerm._strOperator.equals("<") || sqlTerm._strOperator.equals("<=")) {
            return scanFromTheBeginning(sqlTerm, tableInstance);
        }
        throw new DBAppException("Invalid Operator!");
    }

    /**
     * Performs linear scanning to filter entries based on the SQL term condition.
     *
     * @param sqlTerm The SQL term representing the condition.
     * @param tableInstance The table instance to scan.
     * @return A vector of filtered entries.
     * @throws DBAppException if an error occurs during query processing.
    */
    public static Vector<Entry> linearScanning(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        Vector<Entry> filteredResults = new Vector<>();
        for (String strPageName : tableInstance.vecPages) {
            Page page = (Page) DBApp.deserialize(strPageName);
            for (Entry entry : page.vecTuples) {
                String strColType = Meta.fnGetColumnType(sqlTerm._strTableName, sqlTerm._strColumnName);
                String strColValue = (String) (sqlTerm._objValue.toString());
                DBApp.makeInstance(strColType, strColValue);
                if (evaluateCondition(entry.getHtblTuple().get(sqlTerm._strColumnName),sqlTerm._strOperator,sqlTerm._objValue)) {
                    filteredResults.add(entry);
                }
            }
        }
        return filteredResults;
    }

    /**
     * Performs a binary search for entries with a specific clustering key value in a table.
     * @param sqlTerm The SQL term containing the clustering key value.
     * @param tableInstance The instance of the table to search in.
     * @return A vector containing the entries with the specified clustering key value, or an empty vector if not found.
     */
    public static Vector<Entry> binarySearchClustering(SQLTerm sqlTerm, Table tableInstance) {
        int iPageIndex = binarySearchPageLocation((Comparable) sqlTerm._objValue, tableInstance);
        if (iPageIndex == -1) return new Vector<Entry>();
        Page page = (Page) DBApp.deserialize(tableInstance.vecPages.get(iPageIndex));

        int iEntryIdx = binarySearchIndexOfEntry(page.vecTuples, (Comparable) sqlTerm._objValue);
        Vector<Entry> vecEntries = new Vector<>();
        if (iEntryIdx >= 0){
            vecEntries.add(page.vecTuples.get(iEntryIdx));
        }
        return vecEntries;
    }

    /**
     * Searches for the index of an entry within a vector of entries using binary search.
     *
     * @param entries The vector of entries to search.
     * @param id The ID of the entry to search for.
     * @return The index of the entry if found, otherwise -1.
     */
    public static int binarySearchIndexOfEntry(Vector<Entry> entries, Comparable id) {
        int N = entries.size();
        int l = 0, r = N - 1;
        while (l <= r) {
            int mid = l + r >> 1;
            if (entries.get(mid).fnEntryID().equals(id)) {
                return mid;
            }
            if (entries.get(mid).fnEntryID().compareTo(id) > 0) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        return -1;
    }

    /**
     * Searches for the page location using binary search.
     *
     * @param oTarget The target value.
     * @param tableInstance The table instance.
     * @return The index of the page location.
     */
    public static int binarySearchPageLocation(Comparable oTarget, Table tableInstance){
        int N = tableInstance.vecPages.size();
        int l = 0, r = N - 1;
        int location = -1;
        while (l <= r) {
            int mid = l + r >> 1;
            if (oTarget.compareTo(tableInstance.vecMin.get(mid)) < 0) {
                r = mid - 1;
            } else {
                location = mid;
                l = mid + 1;
            }
        }
        return location;
    }

    /**
     * Scans from the end of the table to filter entries based on the SQL term condition.
     *
     * @param sqlTerm The SQL term representing the condition.
     * @param tableInstance The table instance to scan.
     * @return A vector of filtered entries.
     * @throws DBAppException if an error occurs during query processing.
     */
    public static Vector<Entry> scanFromTheEnd(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        Vector<Entry> filteredResults = new Vector<>();
        for (int i = tableInstance.vecPages.size() - 1; i >= 0; i--) {
            Page page = (Page) DBApp.deserialize(tableInstance.vecPages.get(i));
            for (int j = page.vecTuples.size() - 1; j >= 0; j--) {
                Entry entry = page.vecTuples.get(j);
                if (!evaluateCondition(entry.fnEntryID(), sqlTerm._strOperator, sqlTerm._objValue)) {
                    return filteredResults;
                }
                String strColType = Meta.fnGetColumnType(sqlTerm._strTableName, sqlTerm._strColumnName);
                String strColValue = (String) (sqlTerm._objValue.toString());
                DBApp.makeInstance(strColType, strColValue);
                filteredResults.add(entry);
            }
        }
        return filteredResults;
    }

    /**
     * Scans from the beginning of the table to filter entries based on the SQL term condition.
     *
     * @param sqlTerm The SQL term representing the condition.
     * @param tableInstance The table instance to scan.
     * @return A vector of filtered entries.
     * @throws DBAppException if an error occurs during query processing.
     */
    public static Vector<Entry> scanFromTheBeginning(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        Vector<Entry> filteredResults = new Vector<>();
        for (String strPageName : tableInstance.vecPages) {
            Page page = (Page) DBApp.deserialize(strPageName);
            for (Entry entry : page.vecTuples) {
                if (!evaluateCondition(entry.fnEntryID(), sqlTerm._strOperator, sqlTerm._objValue)) {
                    return filteredResults;
                }
                String strColType = Meta.fnGetColumnType(sqlTerm._strTableName, sqlTerm._strColumnName);
                String strColValue = (String) (sqlTerm._objValue.toString());
                DBApp.makeInstance(strColType, strColValue);
                filteredResults.add(entry);
            }
        }
        return filteredResults;
    }

    /**
     * Evaluates a condition between a column value and a given value.
     *
     * @param columnValue The column value.
     * @param operator The comparison operator.
     * @param value The value to compare against.
     * @return true if the condition is satisfied, false otherwise.
     */
    public static boolean evaluateCondition(Object columnValue, String operator, Object value) {
        switch (operator) {
            case "=":
                return columnValue.equals(value);
            case ">":
                return ((Comparable) columnValue).compareTo(value) > 0;
            case "<":
                return ((Comparable) columnValue).compareTo(value) < 0;
            case "<=":
                return ((Comparable) columnValue).compareTo(value) <= 0;
            case ">=":
                return ((Comparable) columnValue).compareTo(value) >= 0;
            case "!=":
                return ((Comparable) columnValue).compareTo(value) != 0;
        }
        return false;
    }
    
    /**
     * Combines two result sets using the specified operator.
     *
     * @param results1 The first result set.
     * @param results2 The second result set.
     * @param operator The logical operator.
     * @return A vector of combined entries.
     * @throws DBAppException if an error occurs during result set combination.
     */
    public static Vector<Entry> combineResults(Vector<Entry> results1, Vector<Entry> results2, String operator) throws DBAppException {
        if (operator.equals("AND")) {
            results1.retainAll(results2);
            return results1;
        }
        if (operator.equals("OR")) {
            Set<Entry> set = new TreeSet<>(results1);
            set.addAll(results2);
            return new Vector<>(set);
        }
        if (operator.equals("XOR")) {
            Set<Entry> set1 = new TreeSet<>(results1);
            Set<Entry> set2 = new TreeSet<>(results2);

            Set<Entry> union = new TreeSet<>(set1);
            union.addAll(set2);
            Set<Entry> intersection = new TreeSet<>(set1);
            intersection.retainAll(set2);
            union.removeAll(intersection);

            return new Vector<>(union);
        }
        throw new DBAppException("Invalid Operator " + operator);
    }
    
}
