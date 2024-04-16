package com.db;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public class Table {
    String strTableName;
    String strClusteringKeyColumn;
    Vector<Page> vecPages;

    static String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    static String file = rootPath + "metadata.csv";

    public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
        vecPages = new Vector<>();
        this.strTableName = strTableName;
        this.strClusteringKeyColumn = strClusteringKeyColumn;
        fnInsertTable(strTableName, strClusteringKeyColumn, htblColNameType);
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
