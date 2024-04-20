package com.db;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Hashtable;
import java.util.Vector;
/**
 * The Meta class provides utility methods for managing metadata of database tables.
 */
public final class Meta {

    /**
     * Searches for metadata of a specific table.
     *
     * @param strTableName The name of the table to search for.
     * @return true if the metadata for the table is found, false otherwise.
     */
    public static boolean searchMetaData(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String line;
            while ((line = brReader.readLine()) != null) {
                if (getTableName(line).equals(strTableName)) {
                    return true;
                }
            }
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
        return false;
    }
    /**
     * Retrieves information about the columns of a specific table.
     *
     * @param strTableName The name of the table.
     * @return A vector containing information about the columns of the table.
     */
    public static Vector<String> getTableInfo(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            Vector<String> tableInfo = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                if (getTableName(columnInfo).equals(strTableName)) {
                    tableInfo.add(columnInfo);
                }
            }
            return tableInfo;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if the provided hashtable contains a value for the clustering key of the specified table.
     *
     * @param strTableName     The name of the table.
     * @param htblColNameValue The hashtable containing column names and their corresponding values.
     * @return true if the clustering key value is present, false otherwise.
     */
    public static boolean checkClusteringKey(String strTableName, Hashtable<String,Object> htblColNameValue) {
        String clusteringKey = getTableClusteringKey(strTableName);
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

    /**
     * Checks if the values in the provided hashtable match the types defined for the columns in the table.
     *
     * @param strTableName     The name of the table.
     * @param htblColNameValue The hashtable containing column names and their corresponding values.
     * @return true if the values match the column types, false otherwise.
     * @throws DBAppException if an invalid column name or value type is encountered.
     */
    public static boolean checkTableColumns(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        Hashtable<String, String> columnTypes = new Hashtable<>();
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String colInfo: tableInfo){
            columnTypes.put(getColumnName(colInfo), getColumnType(colInfo));
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
    /**
     * Checks if the provided hashtable contains null values for all columns in the specified table.
     *
     * @param strTableName     The name of the table.
     * @param htblColNameValue The hashtable containing column names and their corresponding values.
     * @return true if all columns have non-null values, false otherwise.
     * @throws DBAppException if an invalid column name or value type is encountered.
     */
    public static boolean checkTableColumnsNull(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        Hashtable<String, String> columnTypes = new Hashtable<>();
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String colInfo: tableInfo){
            columnTypes.put(getColumnName(colInfo), getColumnType(colInfo));
        }
        for (String colName: columnTypes.keySet()){
            if (!htblColNameValue.containsKey(colName)){
                return false;
            }
        }
        return true;
    }
    /**
     * Inserts metadata for a new table into the metadata file.
     *
     * @param strTableName           The name of the table.
     * @param strClusteringKeyColumn The name of the clustering key column.
     * @param htblColNameType       A hashtable containing column names and their corresponding types.
     * @throws DBAppException if metadata for the table already exists.
     */
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
    /**
     * Creates a row of metadata for a table column.
     *
     * @param strTableName        The name of the table.
     * @param bIsClusteringKey    true if the column is a clustering key, false otherwise.
     * @param strColName          The name of the column.
     * @param strType             The type of the column.
     * @return A string representing a row of metadata for the column.
     */
    public static String makeRow(String strTableName, boolean bIsClusteringKey, String strColName, String strType) {
        String[] row = new String[6];
        row[0] = strTableName;
        row[1] = strColName;
        row[2] = strType;
        row[3] = "" + bIsClusteringKey;
        row[4] = row[5] = "null";
        return String.join(",", row);
    }

    /**
     * Deletes metadata and indices of a specified table.
     *
     * @param strTableName The name of the table whose metadata is to be deleted.
     */

    public static void deleteTableMetaData(String strTableName) {
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            Vector<String> data = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                if (!getTableName(columnInfo).equals(strTableName)) {
                    data.add(columnInfo);
                }else {
                    if (!getIndexName(columnInfo).equals("null")){
                        File file = new File(getIndexName(columnInfo) + ".class");
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

    /**
     * Retrieves the names of indexes associated with a specific table.
     *
     * @param strTableName The name of the table.
     * @return A vector containing pairs of column names and their associated index names.
     */
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
                String strIndexName = getIndexName(strColInfo);
                String strColName = getColumnName(strColInfo);
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


    /**
     * Displays metadata stored in the database file.
     */
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

    /**
     * Displays data stored in the index with the specified name.
     *
     * @param strIndexName The name of the index.
     */
    public static void showIndexData(String strIndexName) {
        Index index = (Index) DBApp.deserialize(strIndexName);
        System.out.println(index);
    }

    /**
     * Displays data stored in the table with the specified name.
     *
     * @param strTableName The name of the table.
     */
    public static void showTableData(String strTableName) {
        Table table = (Table) DBApp.deserialize(strTableName);
        System.out.println(table);
    }

    /**
     * Creates an index on a specified column of a table.
     *
     * @param strTableName  The name of the table.
     * @param strColName    The name of the column to create the index on.
     * @param strIndexName  The name of the index to be created.
     * @throws DBAppException if an index with the same name already exists for the column.
     */
    public static void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException{
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            Vector<String> data = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                String[] info = columnInfo.split(",");
                if (getTableName(columnInfo).equals(strTableName) && getColumnName(columnInfo).equals(strColName)) {
                    if(!getIndexName(columnInfo).equals("null")) {
                        throw new DBAppException("Index " + getIndexName(columnInfo) + " already exists!");
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

    /**
     * Deletes an index associated with a specified column of a table.
     *
     * @param strTableName  The name of the table.
     * @param strColName    The name of the column whose index is to be deleted.
     * @param strIndexName  The name of the index to be deleted.
     * @throws DBAppException if the index does not exist for the specified column.
     */
    public static void deleteIndex(String strTableName, String strColName, String strIndexName) throws DBAppException{
        try {
            BufferedReader brReader = new BufferedReader(new FileReader(DBApp.file));
            String columnInfo;
            Vector<String> data = new Vector<>();
            while ((columnInfo = brReader.readLine()) != null) {
                String[] info = columnInfo.split(",");
                if (getTableName(columnInfo).equals(strTableName) && getColumnName(columnInfo).equals(strColName)) {
                    if(getIndexName(columnInfo).equals(strIndexName)) {
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

    /**
     * Retrieves information about a specific column in a table.
     *
     * @param strTableName The name of the table.
     * @param strColName   The name of the column.
     * @return An array containing information about the column (TableName, ColumnName, ColumnType, ClusteringKey, IndexName, IndexType).
     */
    private static String[] getTableColumnInfo(String strTableName, String strColName) {
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String columnInfo: tableInfo){
            if (getColumnName(columnInfo).equals(strColName))
                return columnInfo.split(",");
        }
        return null;
    }

    /**
     * Retrieves the name of the clustering key column for a specified table.
     *
     * @param strTableName The name of the table.
     * @return The name of the clustering key column, or null if no clustering key is defined.
     */
    public static String getTableClusteringKey(String strTableName) {
        Vector<String> tableInfo = getTableInfo(strTableName);
        for (String columnInfo: tableInfo){
            if (isClusteringKey(columnInfo)) {
                return getColumnName(columnInfo);
            }
        }
        return null;
    }

    /**
     * Retrieves the index name associated with a specified column in a table.
     *
     * @param strTableName The name of the table.
     * @param strColName   The name of the column.
     * @return The name of the index associated with the column, or "null" if no index is associated.
     */
    public static String getColumnIndex(String strTableName, String strColName) {
        return getTableColumnInfo(strTableName,strColName)[4];
    }

    /**
     * Checks if an index is associated with a specified column in a table.
     *
     * @param strTableName The name of the table.
     * @param strColName   The name of the column.
     * @return true if an index is associated with the column, false otherwise.
     */
    public static boolean haveColumnIndex(String strTableName, String strColName) {
        return !getColumnIndex(strTableName, strColName).equals("null");
    }

    /**
     * Checks if a specified column exists in a table.
     *
     * @param strTableName The name of the table.
     * @param strColName   The name of the column.
     * @return true if the column exists in the table, false otherwise.
     * @throws DBAppException if an invalid column name is provided.
     */
    public static boolean checkTableColumn(String strTableName, String strColName) throws DBAppException{
        String[] strColumn = getTableColumnInfo(strTableName, strColName);
        return strColumn != null;
    }

    /**
     * Retrieves the type of a specified column in a table.
     *
     * @param strTableName The name of the table.
     * @param strColName   The name of the column.
     * @return The type of the column.
     * @throws DBAppException if an invalid column name is provided.
     */
    public static String getColumnType(String strTableName, String strColName) throws DBAppException{
        String[] strColumn = getTableColumnInfo(strTableName, strColName);
        return strColumn[2];
    }

    /**
     * Retrieves the table name from a given column information string.
     *
     * @param strColumnInfo The column information string in the format (TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType).
     * @return The name of the table.
     */
    public static String getTableName(String strColumnInfo){
        return strColumnInfo.split(",")[0];
    }
    /**
     * Retrieves the column name from a given column information string.
     *
     * @param strColumnInfo The column information string in the format (TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType).
     * @return The name of the column.
     */
    public static String getColumnName(String strColumnInfo){
        return strColumnInfo.split(",")[1];
    }
    /**
     * Retrieves the type of a column from a given column information string.
     *
     * @param strColumnInfo The column information string in the format (TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType).
     * @return The type of the column.
     */
    public static String getColumnType(String strColumnInfo){
        return strColumnInfo.split(",")[2];
    }
    /**
     * Checks if a specified column is a clustering key based on the provided column information string.
     *
     * @param strColumnInfo The column information string in the format (TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType).
     * @return true if the column is a clustering key, false otherwise.
     */
    public static boolean isClusteringKey(String strColumnInfo){
        return new Boolean(strColumnInfo.split(",")[3]);
    }
    /**
     * Retrieves the index name associated with a column from the provided column information string.
     *
     * @param strColumnInfo The column information string in the format (TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType).
     * @return The name of the index associated with the column.
     */
    public static String getIndexName(String strColumnInfo){
        return strColumnInfo.split(",")[4];
    }


    /**
     * Retrieves the index type associated with a column from the provided column information string.
     *
     * @param strColumnInfo The column information string in the format (TableName,ColumnName,ColumnType,ClusteringKey,IndexName,IndexType).
     * @return The type of the index associated with the column.
     */
    public static String getIndexType(String strColumnInfo){
        return strColumnInfo.split(",")[5];
    }

    /**
     * Maps column names to their associated index names for a specified table.
     *
     * @param strTableName The name of the table.
     * @return A hashtable mapping column names to their associated index names.
     */
    public static Hashtable<String,String> mapColumnToIndexName(String strTableName){
        Vector<String> vecTableInfo = getTableInfo(strTableName);
        Hashtable<String,String> result = new Hashtable<>();
        for(String strColumnInfo :vecTableInfo)
            if (!getIndexName(strColumnInfo).equals("null"))
                result.put(getColumnName(strColumnInfo), getIndexName(strColumnInfo));
        return result;
    }
}

/**
 * Represents a pair of column name and index name.
 */
class PairOfIndexColName {
    public String strColumnName;
    public String strIndexName;
    PairOfIndexColName(String strColumnName,String strIndexName){
        this.strColumnName = strColumnName;
        this.strIndexName = strIndexName;
    }

}
