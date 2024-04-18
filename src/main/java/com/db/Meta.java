package com.db;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public final class Meta {

    /**
     * MetaData helper methods
     */
    public static boolean fnSearchMetaData(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            while ((line = brReader.readLine()) != null) {
                String[] elements = line.split(",");
                if (elements[0].equals(strTableName)) {
                    return true;
                }
            }
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public static void fnInsertTableMetaData(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
        if (fnSearchMetaData(strTableName)) {
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
        String[] row = new String[6];
        row[0] = strTableName;
        row[1] = strColName;
        row[2] = strType;
        row[3] = "" + bIsClusteringKey;
        row[4] = row[5] = "null";
        return String.join(",", row);
    }

    public static void deleteTableMetaData(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            Vector<String> data = new Vector<>();
            while ((line = brReader.readLine()) != null) {
                String[] elements = line.split(",");
                if (!elements[0].equals(strTableName)) {
                    data.add(line);
                }else {
                    if (!elements[4].equals("null")){
                        File file = new File(elements[4] + ".class");
                        file.delete();
                    }
                }
            }
            FileWriter writer = new FileWriter(DBApp.file, false);
            for (String record: data){
                writer.write(record + '\n');
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void showMetaData() {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            while ((line = brReader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void fnUpdateTableMetaData(String strTableName, String strColName, String strIndexName) throws DBAppException{
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            Vector<String> data = new Vector<>();
            while ((line = brReader.readLine()) != null) {
                String[] elements = line.split(",");
                if (elements[0].equals(strTableName) && elements[1].equals(strColName)) {
                    if(!elements[4].equals("null")) {
                        throw new DBAppException("Index " + elements[4] + " already exists!");
                    }
                    elements[4] = strIndexName;
                    elements[5] = "B+tree";
                }
                data.add(String.join(",", elements));
            }
            FileWriter writer = new FileWriter(DBApp.file, false);
            for (String record: data){
                writer.write(record + '\n');
            }
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String[] fnGetTableColumn(String strTableName, String strColName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            while ((line = brReader.readLine()) != null) {
                String[] elements = line.split(",");
                if (elements[0].equals(strTableName) && elements[1].equals(strColName)) {
                    return elements;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static String fnGetTableClusteringKey(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            while ((line = brReader.readLine()) != null) {
                String[] elements = line.split(",");
                if (elements[0].equals(strTableName) && elements[3].equals("true")) {
                    return elements[1];
                }
            }
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void fnCheckColType(String strTableName, String strColName, String strColType) throws DBAppException{
        if (!fnGetColumnType(strTableName, strColName).equals(strColType))
            throw new DBAppException("Invalid Column Value!");
    }
    public static String fnGetColumnIndex(String strTableName, String strColName) {
        String[] strColumn = fnGetTableColumn(strTableName,strColName);
        return strColumn[4];
    }

    public static boolean fnHaveColumnIndex(String strTableName, String strColName) throws DBAppException{
        String[] strColumn = fnGetTableColumn(strTableName,strColName);
        return strColumn[3].equalsIgnoreCase("true");
    }

    public static boolean fnCheckTableColumn(String strTableName, String strColName) throws DBAppException{
        String[] strColumn = fnGetTableColumn(strTableName, strColName);
        return strColumn != null;
    }

    public static String fnGetColumnType(String strTableName, String strColName) throws DBAppException{
        String[] strColumn = fnGetTableColumn(strTableName, strColName);
        return strColumn[2];
    }
}
