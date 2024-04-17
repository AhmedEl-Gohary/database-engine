package com.db;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable{
    String strTableName;
    String strClusteringKeyColumn;
    Vector<String> vecPages;
    Vector<Comparable> vecMax,vecMin;

    static String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    static String file = rootPath + "metadata.csv";

    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException {
        vecPages = new Vector<>();
        vecMax = new Vector<>();
        vecMin = new Vector<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        fnInsertTable(strTableName, strClusteringKeyColumn, htblColNameType);
    }
    public void fnInsertNewPage(Hashtable<String, Object> htblColNameValue){
        int iPageNumber = vecPages.size();
        vecPages.add(this.strTableName + vecPages.size());
        vecMin.add((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        vecMax.add((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Page pageInstance = new Page(this.strTableName, iPageNumber);
        DBApp.fnSerialize(pageInstance, strTableName + iPageNumber);
    }
    public void fnInsertEntry(Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (htblColNameValue.get(this.strClusteringKeyColumn) == null) {
            throw new DBAppException("Clustering Key cannot be null!");
        }
        int iPageNumber = fnBSPageLocation((Comparable) htblColNameValue.get(this.strClusteringKeyColumn));
        Entry entryInstance= new Entry(htblColNameValue, this.strClusteringKeyColumn);
        while(entryInstance != null) {
            if (iPageNumber == vecPages.size()) {
                fnInsertNewPage(htblColNameValue);
            }
            Page pageInstance = (Page) DBApp.fnDeserialize(strTableName + iPageNumber);
            entryInstance = pageInstance.fnInsertEntry(entryInstance);
            vecMax.set(iPageNumber, pageInstance.vecTuples.lastElement().fnEntryID());
            vecMin.set(iPageNumber, pageInstance.vecTuples.firstElement().fnEntryID());
            DBApp.fnSerialize(pageInstance, strTableName + iPageNumber);

            iPageNumber++;
        }

    }
    public int fnBSPageLocation(Comparable oTarget){
        int l = 0;
        int hi = vecPages.size()-1;
        int ans = hi+1;
        while(hi>=l){
            int mid = (hi+l)/2;
            if(vecMax.get(mid).compareTo(oTarget)>=0 && vecMin.get(mid).compareTo(oTarget)<=0) {
                return mid;
            }
            if(vecMin.get(mid).compareTo(oTarget)>0) {
                ans = mid- 1;
                l = mid - 1;
            }
            else hi = mid+1;
        }
        return ans;

    }
    public static boolean fnSearchInMetaData(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(file));
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
            FileWriter writer = new FileWriter(file, true); // Append mode (optional)

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