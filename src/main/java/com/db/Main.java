package com.db;


import com.btree.BTree;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class Main {
    public static void main(String[] args) throws IOException {


        String strTableName = "Student";
        Hashtable htblColNameType = new Hashtable();
        htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        try {
            DBApp dbApp = new DBApp();
            dbApp.createTable(strTableName, "id", htblColNameType);
            HashSet<Integer> hs = new HashSet<>();
            Hashtable<String, Object> ht = new Hashtable<>();
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

            ht.put("name", "abdelrahim");
            ht.put("id", 4);
            ht.put("gpa", 1);
            dbApp.insertIntoTable(strTableName, ht);

            ht.put("name", "gohary");
            ht.put("id", 5);
            ht.put("gpa", 3);
            dbApp.insertIntoTable(strTableName, ht);
            dbApp.createIndex(strTableName,"gpa","Index");
            Index index = (Index)DBApp.fnDeserialize("Index");
            System.out.println(index.findMaxInIndex());
            System.out.println(index.findMinInIndex());
            System.out.println(index.findGreaterThanKey(2));
            System.out.println(index.findGreaterThanOrEqualKey(1));
            System.out.println(index.findLessThanOrEqualKey(2));
            DBApp.fnSerialize(index,"Index");


        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
    }
}
