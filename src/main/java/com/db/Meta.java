package com.db;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;
import java.util.Vector;

public final class Meta {

    /**
     * MetaData helper methods
     * Layout : TableName,ColumnName, ColumnType, ClusteringKey, IndexName, IndexType
     */
    public static boolean searchMetaData(String strTableName) {
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

    public static Vector<String> getTableInfo(String strTableName) {
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

    public static boolean checkClusteringKey(String strTableName, Hashtable<String,Object> htblColNameValue) {
        String clusteringKey = fnGetTableClusteringKey(strTableName);
        return htblColNameValue.get(clusteringKey) != null;
    }

    private static boolean isSameType(String strValue, String strTypeName){
        try {
            Class<?> className = Class.forName(strTypeName);
            Constructor<?> constructor = className.getConstructor(String.class);
            Object oValue = constructor.newInstance(strValue);
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            return false;
        }
    }
    public static boolean checkTableColumns(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        Hashtable<String, String> columnTypes = new Hashtable<>();
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String colInfo: tableInfo){
            columnTypes.put(fnGetColumnName(colInfo), fnGetColumnType(colInfo));
        }
        for (String colName: htblColNameValue.keySet()){
            if (!columnTypes.containsKey(colName)){
                throw new DBAppException("Invlid Column Name \'" + colName + "\'");
            }
            Object colValue = htblColNameValue.get(colName);
            if (colValue != null && !isSameType(colValue.toString(), columnTypes.get(colName))){
                throw new DBAppException("Invlid Column \'" + colName + "\' Value \'" + colName + "\'");
            }
        }
        return true;
    }
    public static boolean checkTableColumnsNull(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        Hashtable<String, String> columnTypes = new Hashtable<>();
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String colInfo: tableInfo){
            columnTypes.put(fnGetColumnName(colInfo), fnGetColumnType(colInfo));
        }
        for (String colName: columnTypes.keySet()){
            if (!htblColNameValue.containsKey(colName)){
                return false;
            }
        }
        return true;
    }

    public static Vector<String> getTableColumns(String strTableName) {
        Vector<String> table = getTableInfo(strTableName);
        Vector<String> tableColumns = new Vector<>();
        for (String columnInfo : table) {
            if (fnGetTableName(columnInfo).equals(strTableName)) {
                tableColumns.add(fnGetColumnName(columnInfo));
            }
        }
        return tableColumns;
    }

    public static void insertTableMetaData(String strTableName, String strClusteringKeyColumn, Hashtable<String,String> htblColNameType) throws DBAppException {
        if (searchMetaData(strTableName)) {
            throw new DBAppException("Table data is already inserted");
        }
        // Table Name, Column Name, Column Type, ClusteringKey, IndexName,IndexType
        try {
            FileWriter writer = new FileWriter(DBApp.file, true); // Append mode (optional)

            for (String strColName : htblColNameType.keySet()) {
                writer.write(makeRow(strTableName, strColName.equals(strClusteringKeyColumn), strColName,
                        htblColNameType.get(strColName)) + "\n");
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String makeRow(String strTableName, boolean bIsClusteringKey, String strColName, String strType) {
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

    public static Vector<PairOfIndexColName> getIndexesNamesInTable(String strTableName)  {
        try {
            Vector<PairOfIndexColName> result = new Vector<>();
            BufferedReader brReader = null;
            brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            while ((columnInfo = brReader.readLine()) != null) {
                String[] row = columnInfo.split(",");
                if (row[0].equals(strTableName)) {
                    if (!row[4].equals("null")) {
                        result.add(new PairOfIndexColName(row[1], row[4]));
                    }
                }
            }
            Vector<String> vecTableInfo = getTableInfo(strTableName);
            for(String strColInfo:vecTableInfo){
                String strIndexName = fnGetIndexName(strColInfo);
                String strColName = fnGetColumnName(strColInfo);
                if (!strIndexName.equals("null"))
                    result.add(new PairOfIndexColName(strColName,strIndexName));
            }
            return result;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }



    public static void showMetaData() {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            while ((columnInfo = brReader.readLine()) != null) {
                System.out.println(columnInfo);
            }
            System.out.println();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void showIndexData(String strIndexName) {
        Index index = (Index) DBApp.deserialize(strIndexName);
        System.out.println(index);
    }

    public static void showTableData(String strTableName) {
        Table table = (Table) DBApp.deserialize(strTableName);
        System.out.println(table);
    }
    public static void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException{
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

    public static void deleteIndex(String strTableName, String strColName, String strIndexName) throws DBAppException{
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            Vector<String> data = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                String[] info = columnInfo.split(",");
                if (fnGetTableName(columnInfo).equals(strTableName) && fnGetColumnName(columnInfo).equals(strColName)) {
                    if(fnGetIndexName(columnInfo).equals(strIndexName)) {
                        info[4] = "null";
                        info[5] = "null";
                    }
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
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String columnInfo: tableInfo){
            if (fnGetColumnName(columnInfo).equals(strColName))
                return columnInfo.split(",");
        }
        return null;
    }

    public static String fnGetTableClusteringKey(String strTableName) {
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String columnInfo: tableInfo){
            if (fnIsClusteringKey(columnInfo)) {
                return fnGetColumnName(columnInfo);
            }
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
    public static Hashtable<String,String> fnMapColumnToIndexName (String strTableName){
        Vector<String> vecTableInfo = getTableInfo(strTableName);
        Hashtable<String,String> result = new Hashtable<>();
        for(String strColumnInfo :vecTableInfo)
            if (!fnGetIndexName(strColumnInfo).equals("null"))
                result.put(fnGetColumnName(strColumnInfo),fnGetIndexName(strColumnInfo));
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
