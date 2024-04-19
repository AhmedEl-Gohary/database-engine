package com.db;


import com.btree.BTree;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

public class Main {
    public static void main(String[] args) throws IOException {
//        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
//        String appConfigPath = rootPath + "DBApp.config";
//
//        Properties p = new Properties();
//        p.load(new FileInputStream(appConfigPath));
//
//        String rows = p.getProperty("MaximumRowsCountinPage");
//        System.out.println(rows);
            BTree<Integer, Integer> btree = new BTree<>();
            Random r = new Random();
            for(int i=0;i<12;i++){
                int x = r.nextInt(3);
                int y = r.nextInt(100);
                int z = r.nextInt(3);
//                Vector<Integer> v = new Vector<>();
//                v.add(y);v.add(z);
                btree.insert(x,y);
                System.out.println(x+" "+y+" "+z);
            }
            System.out.println(btree.findMinInTree());
            System.out.println(btree.findMaxInTree());




    }
}
