package com.db;

import com.btree.BTree;

public class PrimaryIndex<TKey extends Comparable<TKey>> implements java.io.Serializable {
    BTree<TKey, String> btree;
    String strIndexName, strTableName, strClusteringKey;

    private void addPage(String strPageName) {
        Page pageInstance = (Page) DBApp.fnDeserialize(strPageName);
        for (Entry entry : pageInstance.vecTuples) {
            btree.insert((TKey) entry.getColumnValue(strClusteringKey), strPageName);
        }
    }

    private void build() {
        Table tableInstance = (Table) DBApp.fnDeserialize(strTableName);
        for (String page : tableInstance.vecPages) {
            addPage(page);
        }
    }

    public PrimaryIndex(String strIndexName, String strTableName, String strClusteringKeyValue) {
        this.strIndexName = strIndexName;
        this.strTableName = strTableName;
        this.strClusteringKey = strClusteringKeyValue;
        btree = new BTree<>();
        build();
    }


}
