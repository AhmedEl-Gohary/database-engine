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
        if (fnIsExistingFile(strTableName))
            throw new DBAppException("This table already exists!");
        Table tableInstance = new Table(strTableName, strClusteringKeyColumn, htblColNameType);
        Meta.fnInsertTableMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
        fnSerialize(tableInstance, strTableName);
    }


    // following method creates a B+tree index
    public void createIndex(String strTableName, String strColName, String strIndexName) throws DBAppException {
        if (!fnIsExistingFile(strTableName))
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
        fnSerialize(index, strIndexName);
        Meta.fnUpdateTableMetaData(strTableName, strColName, strIndexName);
    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {
        if (!fnIsExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");
        Table tableInstance = (Table) fnDeserialize(strTableName);
        String strPageName = tableInstance.fnInsertEntry(htblColNameValue);
        String strClusteringKey = Meta.fnGetTableClusteringKey(strTableName);
        Vector<PairOfIndexColName> vecIndexesNames = Meta.fnGetIndexesNamesInTable(strTableName);
        for(PairOfIndexColName pairOfIndexColName:vecIndexesNames) {
            Index indexInstance = (Index)fnDeserialize(pairOfIndexColName.strIndexName);
            indexInstance.insert((Comparable) htblColNameValue.get(pairOfIndexColName.strColumnName),
                    new Pair((Comparable) htblColNameValue.get(strClusteringKey),strPageName));
            fnSerialize(indexInstance,pairOfIndexColName.strIndexName);
        }
        fnSerialize(tableInstance, strTableName);
    }


    // following method updates one row only
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    // strClusteringKeyValue is the value to look for to find the row to update.
    public void updateTable(String strTableName, String strClusteringKeyValue, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
        if (!fnIsExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");

        String strClusteringKeyName = Meta.fnGetTableClusteringKey(strTableName);
        String strClusteringKeyType = Meta.fnGetColumnType(strTableName , strClusteringKeyName);
        Object objClusteringKeyValue = fnMakeInstance(strClusteringKeyType, strClusteringKeyValue);

        Table tableInstance = (Table) fnDeserialize(strTableName);
        Hashtable<String, Object> htblEntryKey = new Hashtable<>();
        htblEntryKey.put(strClusteringKeyName, objClusteringKeyValue);
        tableInstance.fnUpdateEntry(htblEntryKey,htblColNameValue);

        // index part
        Vector<PairOfIndexColName> vecOfPairs = Meta.fnGetIndexesNamesInTable(strTableName);
        for(PairOfIndexColName pair: vecOfPairs){
            Index indexInstance = (Index) fnDeserialize(pair.strIndexName);
            Comparable key = (Comparable) tableInstance.
                                    fnSearchEntryWithClusteringKey(htblEntryKey,strClusteringKeyName).
                                    getColumnValue(pair.strColumnName);
            Vector<Pair> toBeChanged = indexInstance.delete(key,(Comparable) objClusteringKeyValue);
            for (Pair pairtoBeChanged : toBeChanged) {
                indexInstance.insert((Comparable)htblColNameValue.get(pair.strColumnName),pairtoBeChanged);
            }
            fnSerialize(indexInstance,pair.strIndexName);

        }
        fnSerialize(tableInstance, strTableName);
    }
    // name = "ahmed" and age = 20 and gender = "male"


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search
    // to identify which rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {

        if (!fnIsExistingFile(strTableName))
            throw new DBAppException("This table doesn't exist");

        for (String strColumnName : htblColNameValue.keySet()) {
            if (!Meta.fnCheckTableColumn(strTableName, strColumnName)) {
                throw new DBAppException("Column named: " + strColumnName + " doesn't exist!") ;
            }
        }

        // here i am, therefore i code

        Vector<Entry> vecResults = new Vector<>();
        Vector<PairOfIndexColName> vecOfPairs = Meta.fnGetIndexesNamesInTable(strTableName);

        Table tableInstance = (Table)fnDeserialize(strTableName);
        String strClusteringKeyName = Meta.fnGetTableClusteringKey(strTableName);
        if(htblColNameValue.containsKey(strClusteringKeyName)){
            Entry entryInstance = tableInstance.fnSearchEntryWithClusteringKey(htblColNameValue,strClusteringKeyName);
            vecResults.add(entryInstance);
        }
        else{
            Vector<String> tableInfo = Meta.fnGetTableInfo(strTableName);
            if(vecOfPairs.isEmpty()){
                // not index found;
                // linear search
                for (String strPageName : tableInstance.vecPages) {
                    Page page = (Page) fnDeserialize(strPageName);
                    for (Entry entry : page.vecTuples) {
                        boolean ok = true;
                        for(String strColName:htblColNameValue.keySet())
                            ok &= entry.getColumnValue(strColName).equals(htblColNameValue.get(strColName));
                        if(ok)
                            vecResults.add(entry);
                    }
                    fnSerialize(page,strPageName);
                }

            }
            else{

                Index indexInstance = (Index) fnDeserialize(vecOfPairs.get(0).strIndexName);
                Vector<Pair> vecOfSubResults = indexInstance.search((Comparable)htblColNameValue.get(vecOfPairs.get(0).strColumnName));
                for(Pair pair:vecOfSubResults) {
                    Entry entry = tableInstance.fnSearchInPageWithClusteringKey(pair);
                    boolean ok = true;
                    for(String strColName:htblColNameValue.keySet())
                        ok &= entry.getColumnValue(strColName).equals(htblColNameValue.get(strColName));
                    if(ok)
                        vecResults.add(entry);
                }
                fnSerialize(indexInstance,vecOfPairs.get(0).strIndexName);
                for(PairOfIndexColName pair:vecOfPairs) {
                    indexInstance = (Index)fnDeserialize(pair.strIndexName);
                    for(Entry entry : vecResults){
                        indexInstance.delete((Comparable) entry.getColumnValue(pair.strColumnName), entry.fnEntryID());
                    }
                    fnSerialize(indexInstance,pair.strIndexName);
                }
            }
        }
        for(Entry entry: vecResults)
            tableInstance.fnDeleteEntry(entry);


        fnSerialize(tableInstance, strTableName);


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
        if (!sqlTerm._strOperator.equals("!=") && sqlTerm._strColumnName.equals(tableInstance.strClusteringKeyColumn)) {
            return clusteringQueries(sqlTerm, tableInstance);
        } else if (!sqlTerm._strOperator.equals("!=") && Meta.fnHaveColumnIndex(sqlTerm._strTableName, sqlTerm._strColumnName)) {
            Index index = (Index) fnDeserialize(Meta.fnGetColumnIndex(sqlTerm._strTableName, sqlTerm._strColumnName));

        } else {
            return linearScanning(sqlTerm, tableInstance);
        }
        return null;
    }

    private Vector<Entry> indexQueries(SQLTerm sqlTerm, Index index, Table tableInstance) {
        if (sqlTerm._strOperator.equals("=")) {
            return indexPointQuery(sqlTerm, index);
        }
        if (sqlTerm._strOperator.equals(">") || sqlTerm._strOperator.equals(">=")) {

        }
        return null;
    }

    private Vector<Entry> indexPointQuery(SQLTerm sqlTerm, Index index) {
        Vector<Entry> resultSet = new Vector<>();
        Vector<Pair> vecPages = index.search((Comparable) sqlTerm._objValue);
        for (Pair pair : vecPages) {
            Comparable id = pair.getCmpClusteringKey();
            String pageName = pair.getStrPageName();
            Page page = (Page) fnDeserialize(pageName);
            int iEntryIndex = binarySearchIndexOfEntry(page.vecTuples, id);
            resultSet.add(page.vecTuples.get(iEntryIndex));
        }
        return resultSet;
    }


    private Vector<Entry> clusteringQueries(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        if (sqlTerm._strOperator.equals("=")) {
            return binarySearchClustering(sqlTerm, tableInstance);
        }
        if (sqlTerm._strOperator.equals(">") || sqlTerm._strOperator.equals(">=")) {
            return scanFromTheEnd(sqlTerm, tableInstance);
        }
        return scanFromTheBeginning(sqlTerm, tableInstance);
    }

    private Vector<Entry> linearScanning(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        Vector<Entry> filteredResults = new Vector<>();
        for (String strPageName : tableInstance.vecPages) {
            Page page = (Page) fnDeserialize(strPageName);
            for (Entry entry : page.vecTuples) {
                String strColType = Meta.fnGetColumnType(sqlTerm._strTableName, sqlTerm._strColumnName);
                String strColValue = (String) (sqlTerm._objValue.toString());
                fnMakeInstance(strColType, strColValue);
                if (evaluateCondition(entry.getHtblTuple().get(sqlTerm._strColumnName),sqlTerm._strOperator,sqlTerm._objValue)) {
                    filteredResults.add(entry);
                }
            }
        }
        return filteredResults;
    }

    private Vector<Entry> binarySearchClustering(SQLTerm sqlTerm, Table tableInstance) {
        int iPageIndex = binarySearchPageLocation((Comparable) sqlTerm._objValue, tableInstance);
        if (iPageIndex == -1) return new Vector<Entry>();
        Page page = (Page) fnDeserialize(tableInstance.vecPages.get(iPageIndex));

        int iEntryIdx = binarySearchIndexOfEntry(page.vecTuples, (Comparable) sqlTerm._objValue);
        Vector<Entry> vecEntries = new Vector<>();
        if (iEntryIdx >= 0){
            vecEntries.add(page.vecTuples.get(iEntryIdx));
        }
        return vecEntries;
    }

    private int binarySearchIndexOfEntry(Vector<Entry> entries, Comparable id) {
        int N = entries.size();
        int l = 0, r = N - 1;
        while (l <= r) {
            int mid = l + r >> 1;
            if (entries.get(mid).fnEntryID().equals(id)) {
                return mid;
            }
            if (entries.get(mid).fnEntryID().compareTo(id) > 0) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        return -1;
    }

    private int binarySearchPageLocation(Comparable oTarget, Table tableInstance){
        int N = tableInstance.vecPages.size();
        int l = 0, r = N - 1;
        int location = -1;
        while (l <= r) {
            int mid = l + r >> 1;
            if (oTarget.compareTo(tableInstance.vecMin.get(mid)) < 0) {
                r = mid - 1;
            } else {
                location = mid;
                l = mid + 1;
            }
        }
        return location;
    }

    private Vector<Entry> scanFromTheEnd(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        Vector<Entry> filteredResults = new Vector<>();
        for (int i = tableInstance.vecPages.size() - 1; i >= 0; i--) {
            Page page = (Page) fnDeserialize(tableInstance.vecPages.get(i));
            for (int j = page.vecTuples.size() - 1; j >= 0; j--) {
                Entry entry = page.vecTuples.get(j);
                if (!evaluateCondition(entry.fnEntryID(), sqlTerm._strOperator, sqlTerm._objValue)) {
                    return filteredResults;
                }
                String strColType = Meta.fnGetColumnType(sqlTerm._strTableName, sqlTerm._strColumnName);
                String strColValue = (String) (sqlTerm._objValue.toString());
                fnMakeInstance(strColType, strColValue);
                filteredResults.add(entry);
            }
        }
        return filteredResults;
    }

    private Vector<Entry> scanFromTheBeginning(SQLTerm sqlTerm, Table tableInstance) throws DBAppException {
        Vector<Entry> filteredResults = new Vector<>();
        for (String strPageName : tableInstance.vecPages) {
            Page page = (Page) fnDeserialize(strPageName);
            for (Entry entry : page.vecTuples) {
                if (!evaluateCondition(entry.fnEntryID(), sqlTerm._strOperator, sqlTerm._objValue)) {
                    return filteredResults;
                }
                String strColType = Meta.fnGetColumnType(sqlTerm._strTableName, sqlTerm._strColumnName);
                String strColValue = (String) (sqlTerm._objValue.toString());
                fnMakeInstance(strColType, strColValue);
                filteredResults.add(entry);
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

    public static void fnDeleteFile(String strObjectName){
        File serializedFile = new File(strObjectName);
        if (serializedFile.exists())
            serializedFile.delete();

    }

    public static Object fnMakeInstance(String strColType, String strColValue) throws DBAppException {
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

    private static void removeTable(String strTableName) {
        Table table = (Table) fnDeserialize(strTableName);
        for (String page : table.vecPages) {
            File file = new File(page + ".class");
            file.delete();
        }
        Meta.deleteTableMetaData(strTableName);
        File file = new File(strTableName + ".class");
        file.delete();
    }

    public static void main( String[] args ) throws DBAppException {
        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable( );
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        try {
            DBApp dbApp = new DBApp();
//            dbApp.createTable(strTableName,"id",htblColNameType);
//            HashSet<Integer> hs = new HashSet<>();
//            Hashtable<String,Object> ht = new Hashtable<>();
//            ht.put("name", "ahmed");
//            ht.put("id", 3);
//            ht.put("gpa", 2);
//            dbApp.insertIntoTable(strTableName, ht);
//            ht.clear();
//            ht.put("name", "yasser");
//            ht.put("id", 1);
//            ht.put("gpa", 1);
//            dbApp.insertIntoTable(strTableName, ht);
//            ht.clear();
//            ht.put("name", "tawfik");
//            ht.put("id", 2);
//            ht.put("gpa", 3);
//            dbApp.insertIntoTable(strTableName, ht);
//            ht.clear();
//            dbApp.createIndex(table.strTableName, "name", "test" );

            System.out.println();
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

            ht.put("name", "abdelwahab");
            ht.put("id", 4);
            ht.put("gpa", 3);
            dbApp.insertIntoTable(strTableName, ht);

            ht.put("name", "Gohary");
            ht.put("id", 5);
            ht.put("gpa", 3);
            dbApp.insertIntoTable(strTableName, ht);
            ht.put("name", "hassan");
            ht.put("id", 6);
            ht.put("gpa", 3);
            dbApp.insertIntoTable(strTableName, ht);

            ht.put("name", "elbakly");
            ht.put("id", 7);
            ht.put("gpa", 3);
            dbApp.insertIntoTable(strTableName, ht);

            Table table = (Table) fnDeserialize(strTableName);
            table = (Table) fnDeserialize(strTableName);
            System.out.println(table);
            SQLTerm[] arr = new SQLTerm[1];
            arr[0] = new SQLTerm();
            arr[0]._strColumnName = "id";
            arr[0]._strOperator = "!=";
            arr[0]._strTableName = "Student";
            arr[0]._objValue = 3;
            System.out.println(dbApp.applyCondition(arr[0]));
            removeTable(strTableName);
//            ht.remove("id");
//            ht.put("gpa", 0.7);
//            String strIdxName = "Index";
//            dbApp.createIndex(strTableName, "name", strIdxName);
//            Index<String> index = (Index<String>) fnDeserialize(strIdxName);
//            System.out.println(index.search("yasser"));
//            removeTable(strTableName);


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

        } catch (Throwable e) {
            removeTable(strTableName);
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





}