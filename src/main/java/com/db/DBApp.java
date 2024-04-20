package com.db;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

// TODO: decide on how to compare Strings

public class DBApp {

    static String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
    static String file = rootPath + "metadata.csv";
    public DBApp() {
        init();
    }

    // this does whatever initialization you would like
    // or leave it empty if there is no code you want to
    // execute at application startup
    public void init( ){
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + "DBApp.config";
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(appConfigPath));
        }catch (IOException e){
            System.out.println(e.getMessage());
        }
        Page.iMaxRowsCount = Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
    }


    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary
    // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    public void createTable(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType) throws DBAppException{
        if (isExistingFile(strTableName))
            throw new DBAppException("This table already exists!");
        Table tableInstance = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
        Meta.insertTableMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
        serialize(tableInstance, strTableName);
    }


    // following method creates a B+tree index
    public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {
        if (!isExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist!");
        if (!Meta.fnCheckTableColumn(strTableName, strColName))
            throw new DBAppException("There are no columns with this name in the table!");
        String strColumnType = Meta.fnGetColumnType(strTableName, strColName);
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


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        if (!isExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");
        if (!Meta.checkTableColumnsNull(strTableName, htblColNameValue))
            throw new DBAppException("Missing Columns Values");
        Table tableInstance = (Table) deserialize(strTableName);
        tableInstance.fnInsertEntry(htblColNameValue);
        serialize(tableInstance, strTableName);
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (!isExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");
        if (Meta.checkClusteringKey(strTableName, htblColNameValue))
            throw new DBAppException("Cannot Update Clustering Key");
        Meta.checkTableColumns(strTableName, htblColNameValue);
        String strClusteringKeyName = Meta.fnGetTableClusteringKey(strTableName);
        String strClusteringKeyType = Meta.fnGetColumnType(strTableName , strClusteringKeyName);
        Object objClusteringKeyValue = makeInstance(strClusteringKeyType, strClusteringKeyValue);

        Table tableInstance = (Table) deserialize(strTableName);
        Hashtable<String, Object> htblEntryKey = new Hashtable<>();
        htblEntryKey.put(strClusteringKeyName, objClusteringKeyValue);
        tableInstance.fnUpdateEntry(htblEntryKey,htblColNameValue);

        // index part

        serialize(tableInstance, strTableName);
    }


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
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
        String strClusteringKeyName = Meta.fnGetTableClusteringKey(strTableName);
        if(htblColNameValue.containsKey(strClusteringKeyName)){
            Entry entryInstance = tableInstance.fnSearchEntryWithClusteringKey(htblColNameValue,strClusteringKeyName);
            if (entryInstance.equals(htblColNameValue))vecResults.add(entryInstance);
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
                    Entry entry = tableInstance.fnSearchInPageWithClusteringKey(pair);
                    if (entry.equals(htblColNameValue)) {
                        vecResults.add(entry);
                    }
                }
            }
        }
        for(PairOfIndexColName pair:vecOfPairs) {
            Index indexInstance = (Index) deserialize(pair.strIndexName);
            for(Entry entry : vecResults){
                indexInstance.delete((Comparable) entry.getColumnValue(pair.strColumnName), entry.fnEntryID());
            }
            serialize(indexInstance,pair.strIndexName);
        }
        for(Entry entry: vecResults) {
            tableInstance.fnDeleteEntry(entry);
        }
        serialize(tableInstance, strTableName);
    }

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
        System.out.println(resultSets.peek());
        return resultSets.pop().iterator();
    }

    public static void main( String[] args ) throws DBAppException {
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        try {
            DBApp dbApp = new DBApp();
            System.out.println();
            dbApp.createTable(strTableName,"id",htblColNameType);
            HashSet<Integer> hs = new HashSet<>();
            Hashtable<String,Object> ht = new Hashtable<>();
            ht.put("name", new String("yasser"));
            ht.put("id", new Integer(1));
            ht.put("gpa", new Double(0.9));
            dbApp.insertIntoTable(strTableName, ht);




            ht.put("name", "tawfik");
            ht.put("id", new Integer(2));
            ht.put("gpa", new Double(3));
            dbApp.insertIntoTable(strTableName, ht);
            dbApp.createIndex(strTableName,"id","yasser");
            ht.put("name", new String("yasser"));
            ht.put("id", new Integer(4));
            ht.put("gpa", new Double(2));
            dbApp.insertIntoTable(strTableName, ht);
            ht.put("name", new String("ali"));
            ht.put("id", new Integer(5));
            ht.put("gpa", new Double(2));
            dbApp.insertIntoTable(strTableName, ht);
            Table table= (Table) deserialize(strTableName);
            System.out.println(table);
            ht.remove("id");
            ht.remove("gpa");
            ht.put("name" , "yasser");
            dbApp.deleteFromTable(strTableName ,ht);
            table= (Table) deserialize(strTableName);
            System.out.println(table);
            ht.put("name", new String("alii"));
            ht.put("id", new Integer(7));
            ht.put("gpa", new Double(2));
            dbApp.insertIntoTable(strTableName, ht);
            ht.put("name", new String("aliii"));
            ht.put("id", new Integer(6));
            ht.put("gpa", new Double(2));
            dbApp.insertIntoTable(strTableName, ht);
            ht.put("name", new String("aliii"));
            ht.put("id", new Integer(0));
            ht.put("gpa", new Double(2));
            dbApp.insertIntoTable(strTableName, ht);

            table= (Table) deserialize(strTableName);

            ht.remove("name");
            ht.remove("gpa");
            ht.put("id",7);
            dbApp.deleteFromTable(strTableName ,ht);
            table= (Table) deserialize(strTableName);
            System.out.println(table);
            ht.put("id" , 0);
            dbApp.deleteFromTable(strTableName ,ht);
            ht.put("id" , 2);
            dbApp.deleteFromTable(strTableName ,ht);
            table= (Table) deserialize(strTableName);
            System.out.println(table);


        } catch (Throwable e) {
            e.printStackTrace();
        }finally {
            removeTable(strTableName);
        }
    }
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
    public static void deleteFile(String strObjectName){
        File serializedFile = new File(strObjectName);
        if (serializedFile.exists())
            serializedFile.delete();

    }

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
    public static void removeTable(String strTableName) {
        Table table = (Table) deserialize(strTableName);
        for (String page : table.vecPages) {
            File file = new File(page + ".class");
            file.delete();
        }
        Meta.deleteTableMetaData(strTableName);
        File file = new File(strTableName + ".class");
        file.delete();
    }
    public static void clearTable(String strTableName){
        Table table = (Table) deserialize(strTableName);
        for (String page : table.vecPages) {
            File file = new File(page + ".class");
            file.delete();
        }
        table.clear();
        serialize(table, strTableName);
    }
    public  void clearTableAndIndex(String strTableName) throws DBAppException {
        DBApp.clearTable(strTableName);
        Vector<PairOfIndexColName> vec = Meta.getIndexesNamesInTable(strTableName);
        for(PairOfIndexColName pair:vec){
            Meta.deleteIndex(strTableName, pair.strColumnName, pair.strIndexName);
            createIndex(strTableName,pair.strColumnName, pair.strIndexName);
        }
    }

}
