package com.db;

import com.btree.BTree;

import java.io.Serializable;
import java.util.Vector;

public class Index<TKey extends Comparable<TKey>> implements Serializable {
    BTree<TKey, Vector<Pair>> btree;
    String strIndexName, strTableName, strIndexColumn;

    public Index() {}

    public Index(String strIndexName, String strTableName, String strIndexColumn) {
        this.strIndexName = strIndexName;
        this.strTableName = strTableName;
        this.strIndexColumn = strIndexColumn;
        btree = new BTree<>();
        build();
    }

    private void addPage(String strPageName) {
        Page pageInstance = (Page) DBApp.fnDeserialize(strPageName);
        for (Entry entry : pageInstance.vecTuples) {
            Comparable clusteringKey = (Comparable) entry.fnEntryID();
            TKey key = (TKey) entry.getColumnValue(strIndexColumn);
            insert(key, new Pair(clusteringKey, pageInstance.fnGetPageName()));
        }
    }

    private void build() {
        Table tableInstance = (Table) DBApp.fnDeserialize(strTableName);
        for (String page : tableInstance.vecPages) {
            addPage(page);
        }
    }

    public void insert(TKey key, Pair value) {
        Vector<Pair> curValue = btree.search(key);
        if (curValue == null) { // need to insert
            Vector<Pair> vecEntry = new Vector<>();
            vecEntry.add(value);
            btree.insert(key, vecEntry);
        } else {
            Vector<Pair> toBeInserted = new Vector<>();
            toBeInserted.addAll(curValue);
            toBeInserted.add(value);
            btree.delete(key);
            btree.insert(key, toBeInserted);
        }
    }

    public Vector<Pair> search(TKey key){
        Vector<Pair> targetValue = btree.search(key);
        if (targetValue == null) return null;
        Vector<Pair> result = new Vector<>();
        result.addAll(targetValue);
        return result;
    }

    public String search(TKey key, Comparable strClusteringKeyValue){
        Vector<Pair> targetValue = btree.search(key);
        if (targetValue == null) return null;
        for (Pair pair: targetValue){
            if (pair.getCmpClusteringKey().equals(strClusteringKeyValue))
                return pair.getStrPageName();
        }
        return null;
    }

    public void updatePage(TKey key, Comparable strClusteringKeyValue, String strNewPage) {
        Vector<Pair> curValue = btree.search(key);
        if (curValue == null) return;
        for (Pair pair : curValue) {
            if (strClusteringKeyValue.equals(pair.getCmpClusteringKey())) {
                pair.setStrPageName(strNewPage);
                return;
            }
        }
    }

    public void update(TKey key, TKey newKey) {
        Vector<Pair> curValue = btree.search(key);
        if (curValue == null) return;
        btree.delete(key);
        btree.insert(newKey, curValue);
    }

    public void delete(TKey key, Comparable strClusteringKeyValue) {
        Vector<Pair> curValue = btree.search(key);
        if (curValue == null) return;
        if (strClusteringKeyValue == null) {
            btree.delete(key);
            return;
        }
        Vector<Pair> toBeInserted = new Vector<>();
        toBeInserted.addAll(curValue);
        for (Pair pair : curValue) {
            if (pair.getCmpClusteringKey().equals(strClusteringKeyValue)) {
                toBeInserted.remove(pair);
                btree.delete(key);
                btree.insert(key, toBeInserted);
                return;
            }
        }
    }

}


class Pair implements Serializable {
    private Comparable cmpClusteringKey;
    private String strPageName;

    Pair(Comparable cmpClusteringKey, String strPageNumber) {
        this.cmpClusteringKey = cmpClusteringKey;
        this.strPageName = strPageNumber;
    }

    public Comparable getCmpClusteringKey() {
        return cmpClusteringKey;
    }

    public void setCmpClusteringKey(Comparable cmpClusteringKey) {
        this.cmpClusteringKey = cmpClusteringKey;
    }

    public String getStrPageName() {
        return strPageName;
    }

    public void setStrPageName(String strPageName) {
        this.strPageName = strPageName;
    }

    @Override
    public String toString() {
        return cmpClusteringKey + " " + strPageName;
    }
}