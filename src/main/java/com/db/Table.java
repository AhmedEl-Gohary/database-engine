package com.db;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
    String strTableName;
    String strClusteringKeyColumn;
    Vector<String> vecPages;
    Vector<Comparable> vecMin;

    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
        vecPages = new Vector<>();
        vecMin = new Vector<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        fnInsertTable(strTableName, strClusteringKeyColumn, htblColNameType);
    }
    public void fnInsertNewPage(Hashtable<String, Object> htblColNameValue){
        int iPageNumber = vecPages.size();
        vecPages.add(this.strTableName + vecPages.size());
        vecMin.add((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Page pageInstance = new Page(this.strTableName, iPageNumber);
        DBApp.fnSerialize(pageInstance, strTableName + iPageNumber);
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

    }
    public int fnBSPageLocation(Comparable oTarget){
        int N = vecPages.size();
        int l = 0, r = N - 1;
        int iFirstGoodIdx = N;
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
    public static boolean fnSearchInMetaData(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            while ((line = brReader.readLine()) != null) {
                String[] elements = line.split(",");
                if (elements[0].equals(strTableName)) {
                    return true;
                }
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public static void fnInsertTable(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
        if (fnSearchInMetaData(strTableName)) {
            throw new DBAppException("Table data is already inserted");
        }
        // Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType
        try {
            FileWriter writer = new FileWriter(DBApp.file, true); // Append mode (optional)

            for (String strColName : htblColNameType.keySet()) {
                writer.write(fnMakeRow(strTableName, strColName.equals(strClusteringKeyColumn), strColName,
                        htblColNameType.get(strColName)) + "\n");
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String fnMakeRow(String strTableName, boolean bIsClusteringKey, String strColName, String strType) {
        StringBuilder sb = new StringBuilder();
        sb.append(strTableName).append(",");
        sb.append(strColName).append(",");
        sb.append(strType).append(",");
        sb.append(bIsClusteringKey).append(",");
        sb.append("null").append(",");
        sb.append("null");
        return sb.toString();
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