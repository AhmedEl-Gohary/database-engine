package com.db;

import java.io.*;
import java.util.Hashtable;
import java.util.Vector;

public final class Meta {

    /**
     * MetaData helper methods
     * Layout : TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType
     */
    public static boolean fnSearchMetaData(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            while ((line = brReader.readLine()) != null) {
                if (fnGetTableName(line).equals(strTableName)) {
                    return true;
                }
            }
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    public static Vector<String> fnGetTableInfo(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            Vector<String> tableInfo = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                if (fnGetTableName(columnInfo).equals(strTableName)) {
                    tableInfo.add(columnInfo);
                }
            }
            return tableInfo;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void fnCheckTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        String clusteringKey = fnGetTableClusteringKey(strTableName);
        if (htblColNameValue.get(clusteringKey).equals(null))
            throw new DBAppException("Clustering Key " + clusteringKey + " Cannot Be Null");
        Vector<String> columnNames = fnGetTableColumns(strTableName);
        //TODO: complete check
    }

    public static Vector<String> fnGetTableColumns(String strTableName) {
        Vector<String> table = fnGetTableInfo(strTableName);
        Vector<String> tableColumns = new Vector<>();
        for (String columnInfo : table) {
            if (fnGetTableName(columnInfo).equals(strTableName)) {
                tableColumns.add(fnGetColumnName(columnInfo));
            }
        }
        return tableColumns;
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
            String columnInfo;
            Vector<String> data = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                if (!fnGetTableName(columnInfo).equals(strTableName)) {
                    data.add(columnInfo);
                }else {
                    if (!fnGetIndexName(columnInfo).equals("null")){
                        File file = new File(fnGetIndexName(columnInfo) + ".class");
                        file.delete();
                    }
                }
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

    public static Vector<PairOfIndexColName> fnGetIndexesNamesInTable(String strTableName){
            Vector<PairOfIndexColName> result = new Vector<>();
            Vector<String> vecTableInfo = fnGetTableInfo(strTableName);
            for(String strColInfo:vecTableInfo){
                String strIndexName = fnGetIndexName(strColInfo);
                String strColName = fnGetColumnName(strColInfo);
                if (!strIndexName.equals("null"))
                    result.add(new PairOfIndexColName(strColName,strIndexName));
            }
            return result;
    }


    public static void showMetaData() {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            while ((columnInfo = brReader.readLine()) != null) {
                System.out.println(columnInfo);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    public static void fnUpdateTableMetaData(String strTableName, String strColName, String strIndexName) throws DBAppException{
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            Vector<String> data = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                String[] info = columnInfo.split(",");
                if (fnGetTableName(columnInfo).equals(strTableName) && fnGetColumnName(columnInfo).equals(strColName)) {
                    if(!fnGetIndexName(columnInfo).equals("null")) {
                        throw new DBAppException("Index " + fnGetIndexName(columnInfo) + " already exists!");
                    }
                    info[4] = strIndexName;
                    info[5] = "B+tree";
                }
                data.add(String.join(",", info));
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

    private static String[] fnGetTableColumnInfo(String strTableName, String strColName) {
        Vector<String> tableInfo = fnGetTableInfo(strTableName);
        for (String columnInfo: tableInfo){
            if (fnGetColumnName(columnInfo).equals(strColName))
                return columnInfo.split(",");
        }
        return null;
    }

    public static String fnGetTableClusteringKey(String strTableName) {
        Vector<String> tableInfo = fnGetTableInfo(strTableName);
        for (String columnInfo: tableInfo){
            if (fnIsClusteringKey(columnInfo))
                return fnGetColumnName(columnInfo);
        }
        return null;
    }

    public static void fnCheckColType(String strTableName, String strColName, String strColType) throws DBAppException{
        if (!fnGetColumnType(strTableName, strColName).equals(strColType))
            throw new DBAppException("Invalid Column Value!");
    }
    public static String fnGetColumnIndex(String strTableName, String strColName) {
        return fnGetTableColumnInfo(strTableName,strColName)[4];
    }

    public static boolean fnHaveColumnIndex(String strTableName, String strColName) {
        return !fnGetColumnIndex(strTableName, strColName).equals("null");
    }

    public static boolean fnCheckTableColumn(String strTableName, String strColName) throws DBAppException{
        String[] strColumn = fnGetTableColumnInfo(strTableName, strColName);
        return strColumn != null;
    }

    public static String fnGetColumnType(String strTableName, String strColName) throws DBAppException{
        String[] strColumn = fnGetTableColumnInfo(strTableName, strColName);
        return strColumn[2];
    }

    //TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType
    public static String fnGetTableName(String strColumnInfo){
        return strColumnInfo.split(",")[0];
    }
    public static String fnGetColumnName(String strColumnInfo){
        return strColumnInfo.split(",")[1];
    }
    public static String fnGetColumnType(String strColumnInfo){
        return strColumnInfo.split(",")[2];
    }
    public static boolean fnIsClusteringKey(String strColumnInfo){
        return new Boolean(strColumnInfo.split(",")[3]);
    }
    public static String fnGetIndexName(String strColumnInfo){
        return strColumnInfo.split(",")[4];
    }
    public static String fnGetIndexType(String strColumnInfo){
        return strColumnInfo.split(",")[5];
    }
    public static Hashtable<String,String> fnMapColumnToIndexName (Vector<String> vecTableInfo){
        Hashtable<String,String> result = new Hashtable<>();
        for(String strColumnInfo :vecTableInfo)
            result.put(strColumnInfo,fnGetIndexName(strColumnInfo));
        return result;
    }
}
  class PairOfIndexColName {
    public String strColumnName;
    public String strIndexName;
    PairOfIndexColName(String strColumnName,String strIndexName){
        this.strColumnName = strColumnName;
        this.strIndexName = strIndexName;
    }


}
