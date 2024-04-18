package com.db;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
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
        if (fnIsExistingFile(strTableName))
            throw new DBAppException("This table already exists!");
        Table tableInstance = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
        fnSerialize(tableInstance, strTableName);
        fnInsertTableMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
    }


    // following method creates a B+tree index
    public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException, ClassNotFoundException {
        if (!fnIsExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist!");
        if (!fnCheckTableColumn(strTableName, strColName))
            throw new DBAppException("There are no columns with this name in the table!");
        String strColumnType = fnGetColumnType(strTableName, strColName);
        Index index;
        String[] tokens = strColumnType.split(".");
        if (tokens[2].equals("Double")) {
            index = new Index<Double>(strIndexName, strTableName, strColName);
        } else if (tokens[2].equals("Integer")) {
            index = new Index<Integer>(strIndexName, strTableName, strColName);
        } else {
            index = new Index<String>(strIndexName, strTableName, strColName);
        }
        fnSerialize(index, strIndexName);
        fnUpdateTableMetaData(strTableName, strColName, strIndexName);
    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException{
        // TODO: update the index part if exists
        if (!fnIsExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");
        Table tableInstance = (Table) fnDeserialize(strTableName);
        tableInstance.fnInsertEntry(htblColNameValue);
        fnSerialize(tableInstance, strTableName);
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        if (!fnIsExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");

        String strClusteringKeyName = fnGetTableClusteringKey(strTableName);
        String strClusteringKeyType = fnGetColumnType(strTableName , strClusteringKeyName);
        Object objClusteringKeyValue = fnMakeInstance(strClusteringKeyType, strClusteringKeyValue);

        Table tableInstance = (Table) fnDeserialize(strTableName);
        Hashtable<String, Object> htblEntryKey = new Hashtable<>();
        htblEntryKey.put(strClusteringKeyName, objClusteringKeyValue);
        tableInstance.updateEntry(htblEntryKey,htblColNameValue);
        fnSerialize(tableInstance, strTableName);
    }
    // name = "ahmed" and age = 20 and gender = "male"


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
        /* cases:
        1. ID is present
        2.
         */
        // TODO: update the index if it exists
        if (!fnIsExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");
        Table tableInstance = (Table) fnDeserialize(strTableName);
        boolean bPrimaryKeyExists = htblColNameValue.containsKey(tableInstance.strClusteringKeyColumn);
        if (bPrimaryKeyExists) {
            
            return;
        }
        for (String strColumnName : htblColNameValue.keySet()) {
            if (fnCheckTableColumn(strTableName, strColumnName)) {
                //TODO:
                return;
            }
        }


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
            resultSets.push(applyCondition(arrSQLTerms[i]));
            while (!operators.isEmpty() && getPrecedence(operators.peek()) >= getPrecedence(strarrOperators[i])) {
                Vector<Entry> set2 = resultSets.pop();
                Vector<Entry> set1 = resultSets.pop();
                String operator = operators.pop();
                resultSets.push(combineResults(set1, set2, operator));
            }
            operators.push(strarrOperators[i]);
        }
        resultSets.push(applyCondition(arrSQLTerms[numberOfTerms - 1]));
        while (!operators.isEmpty()) {
            Vector<Entry> set2 = resultSets.pop();
            Vector<Entry> set1 = resultSets.pop();
            String operator = operators.pop();
            resultSets.push(combineResults(set1, set2, operator));
        }

        System.out.println(resultSets.peek());
        return resultSets.pop().iterator();
    }

    private int getPrecedence(String operator) {
        if (operator.equals("AND")) return 2;
        if (operator.equals("XOR")) return 1;
        return 0;
    }

    private Vector<Entry> applyCondition(SQLTerm sqlTerm) throws DBAppException {
        Table tableInstance = (Table) fnDeserialize(sqlTerm._strTableName);
        if (sqlTerm._strColumnName.equals(tableInstance.strClusteringKeyColumn)) {

        } else if (fnHaveColumnIndex(sqlTerm._strTableName, sqlTerm._strColumnName)) {
            // TODO: complete
        } else {
            return linearScanning(sqlTerm, tableInstance);
        }
        return null;
    }

    private Vector<Entry> linearScanning(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        Vector<Entry> filteredResults = new Vector<>();
        for (String strPageName : tableInstance.vecPages) {
            Page page = (Page) fnDeserialize(strPageName);
            for (Entry entry : page.vecTuples) {
                String strColType = fnGetColumnType(sqlTerm._strTableName, sqlTerm._strColumnName);
                String strColValue = (String) (sqlTerm._objValue.toString());
                fnMakeInstance(strColType, strColValue);
                if (evaluateCondition(entry.getHtblTuple().get(sqlTerm._strColumnName),sqlTerm._strOperator,sqlTerm._objValue)) {
                    filteredResults.add(entry);
                }
            }
        }
        return filteredResults;
    }

    private boolean evaluateCondition(Object columnValue, String operator, Object value) {
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

    private Vector<Entry> combineResults(Vector<Entry> results1, Vector<Entry> results2, String operator) throws DBAppException {
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
        throw new DBAppException("Operator is not valid!");
    }


    public static void main( String[] args ) throws DBAppException {
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        try {
            DBApp dbApp = new DBApp();
            dbApp.createTable(strTableName,"id",htblColNameType);
            HashSet<Integer> hs = new HashSet<>();
            Hashtable<String,Object> ht = new Hashtable<>();
            ht.put("name", "ahmed");
            ht.put("id", 3);
            ht.put("gpa", 2);
            dbApp.insertIntoTable(strTableName, ht);

            ht.put("name", "yasser");
            ht.put("id", 1);
            ht.put("gpa", 1);
            dbApp.insertIntoTable(strTableName, ht);

            ht.put("name", "tawfik");
            ht.put("id", 2);
            ht.put("gpa", 3);
            dbApp.insertIntoTable(strTableName, ht);
            Table table = (Table) fnDeserialize(strTableName);

            ht.remove("id");
            ht.put("gpa", 0.7);
            dbApp.updateTable(strTableName, "2", ht);
            System.out.println(table);
            removeTable(strTableName);


//            SQLTerm[] arr = new SQLTerm[1];
//            arr[0] = new SQLTerm();
//            arr[0]._strColumnName = "gpa";
//            arr[0]._strOperator = "<=";
//            arr[0]._strTableName = "Student";
//            arr[0]._objValue = 2;
//
//            Vector<Entry> vec = dbApp.applyCondition(arr[0]);


//            int x;


//            Table tableInstance = (Table) fnDeserialize("Student");
//            for(int i = 0; i < tableInstance.fnCountPages(); i++){
//                System.out.println("cnt : " + tableInstance.vecCountRows.get(i));
//                System.out.println("min: " +  tableInstance.vecMin.get(i));
//                String page = tableInstance.vecPages.get(i);
//                Page pageInstance = (Page)fnDeserialize(page);
//                System.out.println(pageInstance);
//            }
//            removeTable("Student");

        } catch (DBAppException e) {
            System.out.println(e.getMessage());
        }


//        try{
//            String strTableName = "Student";
//            DBApp	dbApp = new DBApp( );
//
//            Hashtable htblColNameType = new Hashtable( );
//            htblColNameType.put("id", "java.lang.Integer");
//            htblColNameType.put("name", "java.lang.String");
//            htblColNameType.put("gpa", "java.lang.double");
//            dbApp.createTable( strTableName, "id", htblColNameType );
//            dbApp.createIndex( strTableName, "gpa", "gpaIndex" );
//
//            Hashtable htblColNameValue = new Hashtable( );
//            htblColNameValue.put("id", 2343432);
//            htblColNameValue.put("name", "Ahmed Noor");
//            htblColNameValue.put("gpa", 0.95);
//            dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//            htblColNameValue.clear( );
//            htblColNameValue.put("id", 453455);
//            htblColNameValue.put("name", "Ahmed Noor");
//            htblColNameValue.put("gpa", 0.95);
//            dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//            htblColNameValue.clear( );
//            htblColNameValue.put("id", 5674567);
//            htblColNameValue.put("name", "Dalia Noor");
//            htblColNameValue.put("gpa", 1.25);
//            dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//            htblColNameValue.clear( );
//            htblColNameValue.put("id", 23498);
//            htblColNameValue.put("name", "John Noor");
//            htblColNameValue.put("gpa", 1.5);
//            dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//            htblColNameValue.clear( );
//            htblColNameValue.put("id", 78452);
//            htblColNameValue.put("name", "Zaky Noor");
//            htblColNameValue.put("gpa", 0.88);
//            dbApp.insertIntoTable( strTableName , htblColNameValue );
//
//
//            SQLTerm[] arrSQLTerms;
//            arrSQLTerms = new SQLTerm[2];
//            arrSQLTerms[0]._strTableName =  "Student";
//            arrSQLTerms[0]._strColumnName=  "name";
//            arrSQLTerms[0]._strOperator  =  "=";
//            arrSQLTerms[0]._objValue     =  "John Noor";
//
//            arrSQLTerms[1]._strTableName =  "Student";
//            arrSQLTerms[1]._strColumnName=  "gpa";
//            arrSQLTerms[1]._strOperator  =  "=";
//            arrSQLTerms[1]._objValue     =  1.5;
//
//            String[]strarrOperators = new String[1];
//            strarrOperators[0] = "OR";
//            // select * from Student where name = "John Noor" or gpa = 1.5;
//            Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//        }
//        catch(Exception | DBAppException exp){
//            exp.printStackTrace( );
//        }
    }



    public static void fnSerialize(Serializable serObj, String strObjectName){
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

    public static boolean fnIsExistingFile(String strObjectName) {
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

    public static Object fnDeserialize(String strObjectName){
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


    private static void removeTable(String strTableName) {
        Table table = (Table) fnDeserialize(strTableName);
        for (String page : table.vecPages) {
            File file = new File(page + ".class");
            file.delete();
        }
        deleteTableMetaData(strTableName);
        File file = new File(strTableName + ".class");
        file.delete();
    }


    public static Object fnMakeInstance(String strColType, String strColValue) throws DBAppException {
        try {
            Class<?> className = Class.forName(strColType);
            Constructor<?> constructor = className.getConstructor(String.class);
            return constructor.newInstance(strColValue);
        } catch (ClassNotFoundException e) {
            throw new DBAppException("Invalid Column Value!");
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

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
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
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
                }
            }
            FileWriter writer = new FileWriter(DBApp.file, false);
            for (String record: data){
                writer.write(record + '\n');
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
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
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
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
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
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
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
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