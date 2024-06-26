/**
 * The DBApp class represents a simple database application.
 * It provides functionalities to create tables, indexes, insert, update, delete, and select data.
 */

package com.db;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class DBApp {
    /**
     * The rootPath variable stores the path of the current project's root directory.
     */

    static String rootPath = "src//main//resources//";

    /**
     * The file variable stores the path of the metadata file.
     */
    static String file = rootPath + "metadata.csv";
    /**
     * Constructs a DBApp object and initializes the application.
     */
    public DBApp() throws IOException {
        init();
    }

    /**
     * Initializes the application by loading configuration properties.
     */

    public void init( ) throws IOException {
        File f = new File(file);
        f.createNewFile();
        String appConfigPath = rootPath + "DBApp.config";
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(appConfigPath));
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        Page.iMaxRowsCount = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
    }


    /**
     * Creates a new table with the specified parameters.
     *
     * @param strTableName The name of the table.
     * @param strClusteringKeyColumn The name of the primary key and clustering column.
     * @param htblColNameType A Hashtable containing column names and their corresponding data types.
     * @throws DBAppException if the table already exists.
     */
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException{
        if (isExistingFile(strTableName))
            throw new DBAppException("This table already exists!");
        Table tableInstance = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
        Meta.insertTableMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
        serialize(tableInstance, strTableName);
    }

    /**
     * Creates a B+tree index on the specified column of a table.
     *
     * @param strTableName The name of the table.
     * @param strColName The name of the column to index.
     * @param strIndexName The name of the index.
     * @throws DBAppException if the table doesn't exist or the column doesn't exist in the table.
     */

    public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {
        if (!isExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist!");
        if (!Meta.checkTableColumn(strTableName, strColName))
            throw new DBAppException("There are no columns with this name in the table!");
        String strColumnType = Meta.getColumnType(strTableName, strColName);
        String[] tokens = strColumnType.split("\\.");
        Index index;
        if (tokens[2].equals("Double")) {
            index = new Index<Double>(strIndexName, strTableName, strColName);
        } else if (tokens[2].equals("Integer")) {
            index = new Index<Integer>(strIndexName, strTableName, strColName);
        } else {
            index = new Index<String>(strIndexName, strTableName, strColName);
        }
        serialize(index, strIndexName);
        Meta.createIndex(strTableName, strColName, strIndexName);
    }


    /**
     * Inserts a new row into the specified table.
     *
     * @param strTableName The name of the table.
     * @param htblColNameValue A Hashtable containing column names and their corresponding values for the new row.
     * @throws DBAppException if the table doesn't exist or required column values are missing.
     */
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (!isExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");
        if (!Meta.checkTableColumnsNull(strTableName, htblColNameValue))
            throw new DBAppException("Missing Columns Values");
        Table tableInstance = (Table) deserialize(strTableName);
        tableInstance.insertEntry(htblColNameValue);
        serialize(tableInstance, strTableName);
    }


    /**
     * Updates a row in the specified table.
     *
     * @param strTableName The name of the table.
     * @param strClusteringKeyValue The value of the clustering key to identify the row to update.
     * @param htblColNameValue A Hashtable containing column names and their new values.
     * @throws DBAppException if the table doesn't exist, or clustering key cannot be updated.
     */
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (!isExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");
        if (Meta.checkClusteringKey(strTableName, htblColNameValue))
            throw new DBAppException("Cannot Update Clustering Key");
        Meta.checkTableColumns(strTableName, htblColNameValue);
        String strClusteringKeyName = Meta.getTableClusteringKey(strTableName);
        String strClusteringKeyType = Meta.getColumnType(strTableName , strClusteringKeyName);
        Object objClusteringKeyValue = makeInstance(strClusteringKeyType, strClusteringKeyValue);

        Table tableInstance = (Table) deserialize(strTableName);
        Hashtable<String, Object> htblEntryKey = new Hashtable<>();
        htblEntryKey.put(strClusteringKeyName, objClusteringKeyValue);
        tableInstance.updateEntry(htblEntryKey,htblColNameValue);

        // index part

        serialize(tableInstance, strTableName);
    }


    /**
     * Deletes one or more rows from the specified table based on given conditions.
     *
     * @param strTableName The name of the table.
     * @param htblColNameValue A Hashtable containing column names and values for identifying rows to delete.
     * @throws DBAppException if the table doesn't exist or column values are missing.
     */
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

        if (!isExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");

        Meta.checkTableColumns(strTableName, htblColNameValue);

        // here i am, therefore i code
        if(htblColNameValue.isEmpty()){
            clearTableAndIndex(strTableName);
            return;
        }
        Vector<Entry> vecResults = new Vector<>();
        Vector<PairOfIndexColName> vecOfPairs = Meta.getIndexesNamesInTable(strTableName);
        Vector<PairOfIndexColName> vec = new Vector<>();
        for (PairOfIndexColName i : vecOfPairs){
            if (htblColNameValue.containsKey(i.strColumnName)) vec.add(i);
        }
        vecOfPairs = vec;
        Table tableInstance = (Table) deserialize(strTableName);
        String strClusteringKeyName = Meta.getTableClusteringKey(strTableName);
        if(htblColNameValue.containsKey(strClusteringKeyName)){
            Entry entryInstance = tableInstance.searchEntryWithClusteringKey(htblColNameValue,strClusteringKeyName);
            if (entryInstance != null && entryInstance.equals(htblColNameValue))vecResults.add(entryInstance);
        }
        else{
            if(vecOfPairs.isEmpty()){
                // not index found;
                // linear search
                for (String strPageName : tableInstance.vecPages) {
                    Page page = (Page) deserialize(strPageName);
                    for (Entry entry : page.vecTuples) {
                        if (entry.equals(htblColNameValue)) {
                            vecResults.add(entry);
                        }
                    }
                }
            }
            else{
                Index indexInstance = (Index) deserialize(vecOfPairs.get(0).strIndexName);
                Vector<Pair> vecOfSubResults = indexInstance.search((Comparable)htblColNameValue.get(vecOfPairs.get(0).strColumnName));
                for(Pair pair:vecOfSubResults) {
                    Entry entry = tableInstance.searchInPageWithClusteringKey(pair);
                    if (entry.equals(htblColNameValue)) {
                        vecResults.add(entry);
                    }
                }
            }
        }
        for(PairOfIndexColName pair:vecOfPairs) {
            Index indexInstance = (Index) deserialize(pair.strIndexName);
            for(Entry entry : vecResults){
                indexInstance.delete((Comparable) entry.getColumnValue(pair.strColumnName), entry.getClusteringKeyValue());
            }
            serialize(indexInstance,pair.strIndexName);
        }
        for(Entry entry: vecResults) {
            tableInstance.deleteEntry(entry);
        }
        serialize(tableInstance, strTableName);
    }
    /**
     * Selects rows from the specified table based on given SQL terms and operators.
     *
     * @param arrSQLTerms An array of SQLTerm objects representing conditions.
     * @param strarrOperators An array of operators (AND, OR) to combine conditions.
     * @return An iterator over the selected rows.
     * @throws DBAppException if no conditions are provided or if there's a mismatch between operators and terms.
     */

    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        if (arrSQLTerms == null || arrSQLTerms.length == 0) {
            throw new DBAppException("No conditions provided for selection.");
        }
        if (strarrOperators.length != arrSQLTerms.length - 1) {
            throw new DBAppException("Operators and terms mismatch.");
        }

        int numberOfTerms = arrSQLTerms.length;
        Stack<Vector<Entry>> resultSets = new Stack<>();
        Stack<String> operators = new Stack<>();
        for (int i = 0; i < numberOfTerms - 1; i++) {
            resultSets.push(QueryProcessor.applyCondition(arrSQLTerms[i]));
            while (!operators.isEmpty() && QueryProcessor.getPrecedence(operators.peek()) >= QueryProcessor.getPrecedence(strarrOperators[i])) {
                Vector<Entry> set2 = resultSets.pop();
                Vector<Entry> set1 = resultSets.pop();
                String operator = operators.pop();
                resultSets.push(QueryProcessor.combineResults(set1, set2, operator));
            }
            operators.push(strarrOperators[i]);
        }
        resultSets.push(QueryProcessor.applyCondition(arrSQLTerms[numberOfTerms - 1]));
        while (!operators.isEmpty()) {
            Vector<Entry> set2 = resultSets.pop();
            Vector<Entry> set1 = resultSets.pop();
            String operator = operators.pop();
            resultSets.push(QueryProcessor.combineResults(set1, set2, operator));
        }
        return resultSets.pop().iterator();
    }

    public static void main( String[] args ) throws DBAppException {
        String strTableName = "Student";
        try{
            DBApp	dbApp = new DBApp( );

            Hashtable htblColNameType = new Hashtable( );
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.Double");
            dbApp.createTable( strTableName, "id", htblColNameType );
            dbApp.createIndex( strTableName, "gpa", "gpaIndex" );

            Hashtable htblColNameValue = new Hashtable( );
            htblColNameValue.put("id", new Integer( 2343432 ));
            htblColNameValue.put("name", new String("Ahmed Noor" ) );
            htblColNameValue.put("gpa", new Double( 0.95 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );

            htblColNameValue.clear( );
            htblColNameValue.put("id", new Integer( 453455 ));
            htblColNameValue.put("name", new String("Ahmed Noor" ) );
            htblColNameValue.put("gpa", new Double( 0.95 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );

            htblColNameValue.clear( );
            htblColNameValue.put("id", new Integer( 5674567 ));
            htblColNameValue.put("name", new String("Dalia Noor" ) );
            htblColNameValue.put("gpa", new Double( 1.25 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );

            htblColNameValue.clear( );
            htblColNameValue.put("id", new Integer( 23498 ));
            htblColNameValue.put("name", new String("John Noor" ) );
            htblColNameValue.put("gpa", new Double( 1.5 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );

            htblColNameValue.clear( );
            htblColNameValue.put("id", new Integer( 78452 ));
            htblColNameValue.put("name", new String("Zaky Noor" ) );
            htblColNameValue.put("gpa", new Double( 0.88 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );


            SQLTerm[] arrSQLTerms;
            arrSQLTerms = new SQLTerm[2];
            for (int i = 0; i < 2; i++ ) arrSQLTerms[i] = new SQLTerm();
            arrSQLTerms[0]._strTableName =  "Student";
            arrSQLTerms[0]._strColumnName=  "name";
            arrSQLTerms[0]._strOperator  =  "=";
            arrSQLTerms[0]._objValue     =  "John Noor";

            arrSQLTerms[1]._strTableName =  "Student";
            arrSQLTerms[1]._strColumnName=  "gpa";
            arrSQLTerms[1]._strOperator  =  "=";
            arrSQLTerms[1]._objValue     =  new Double( 1.5 );

            String[]strarrOperators = new String[1];
            strarrOperators[0] = "OR";
            // select * from Student where name = "John Noor" or gpa = 1.5;
            Iterator<Entry> resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
            while (resultSet.hasNext()){
                System.out.println(resultSet.next());
            }
        }
        catch(Exception exp){
            exp.printStackTrace( );
        }finally {
            removeTable(strTableName);
        }
    }

    /**
     * Serializes an object to a file.
     *
     * @param serObj The object to serialize.
     * @param strObjectName The name of the object.
     */

    public static void serialize(Serializable serObj, String strObjectName){
        try {
            FileOutputStream fileOut = new FileOutputStream(strObjectName + ".class");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(serObj);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * Deserializes an object from a file.
     *
     * @param strObjectName The name of the object.
     * @return The deserialized object.
     */

    public static Object deserialize(String strObjectName){
        Object oObj = null;
        try {
            FileInputStream fileIn = new FileInputStream(strObjectName + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            oObj =  in.readObject();
            in.close();
            fileIn.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return oObj;
    }

    /**
     * Checks if a file with the specified name exists.
     *
     * @param strObjectName The name of the file.
     * @return true if the file exists, false otherwise.
     */

    public static boolean isExistingFile(String strObjectName) {
        Object oObj = null;
        try {
            FileInputStream fileIn = new FileInputStream(strObjectName + ".class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            oObj = in.readObject();
            in.close();
            fileIn.close();
            return true;
        } catch (FileNotFoundException e) {
            return false;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * Deletes a file with the specified name.
     *
     * @param strObjectName The name of the file to delete.
     */

    public static void deleteFile(String strObjectName){
        File serializedFile = new File(strObjectName);
        if (serializedFile.exists())
            serializedFile.delete();

    }
    /**
     * Instantiates an object of a specified class with a given value.
     *
     * @param strColType The class type.
     * @param strColValue The value to initialize the object.
     * @return The instantiated object.
     * @throws DBAppException if the column type or value is invalid.
     */

    public static Object makeInstance(String strColType, String strColValue) throws DBAppException {
        try {
            Class<?> className = Class.forName(strColType);
            Constructor<?> constructor = className.getConstructor(String.class);
            return constructor.newInstance(strColValue);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("Invalid Column Type " + strColType);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new DBAppException("Invalid Column Value " + strColType);
        }
    }
    /**
     * Removes a table and its associated metadata.
     *
     * @param strTableName The name of the table to remove.
     */

    public static void removeTable(String strTableName) {
        Table table = (Table) deserialize(strTableName);
        try {
            for (String page : table.vecPages) {
                File file = new File(page + ".class");
                file.delete();
            }
            Meta.deleteTableMetaData(strTableName);
            File file = new File(strTableName + ".class");
            file.delete();
        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
    /**
     * Clears all data from a table.
     *
     * @param strTableName The name of the table to clear.
     */

    public static void clearTable(String strTableName){
        Table table = (Table) deserialize(strTableName);
        for (String page : table.vecPages) {
            File file = new File(page + ".class");
            file.delete();
        }
        table.clear();
        serialize(table, strTableName);
    }
    /**
     * Clears all indexes in a given table.
     *
     * @param strTableName The name of the table to clear.
     * @throws DBAppException if an error occurs during index recreation.
     */

    public void clearIndexes(String strTableName) throws DBAppException {
        Vector<PairOfIndexColName> vec = Meta.getIndexesNamesInTable(strTableName);
        for(PairOfIndexColName pair:vec){
            Meta.deleteIndex(strTableName, pair.strColumnName, pair.strIndexName);
            createIndex(strTableName,pair.strColumnName, pair.strIndexName);
        }
    }
    /**
     * Clears all data from a table and rebuilds associated indexes.
     *
     * @param strTableName The name of the table to clear.
     * @throws DBAppException if an error occurs during index recreation.
     */

    public  void clearTableAndIndex(String strTableName) throws DBAppException {
        DBApp.clearTable(strTableName);
        clearIndexes(strTableName);
    }

}
