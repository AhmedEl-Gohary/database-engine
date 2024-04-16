package com.db;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String appConfigPath = rootPath + "DBApp.config";

        Properties p = new Properties();
        p.load(new FileInputStream(appConfigPath));

        String rows = p.getProperty("MaximumRowsCountinPage");
        System.out.println(rows);
    }
}
