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



    private void build() {
        Table tableInstance = (Table) DBApp.fnDeserialize(strTableName);
        for (String page : tableInstance.vecPages) {
            addPage(page);
        }
    }
    private void addPage(String strPageName) {
        Page pageInstance = (Page) DBApp.fnDeserialize(strPageName);
        for (Entry entry : pageInstance.vecTuples) {
            Comparable clusteringKey = (Comparable) entry.fnEntryID();
            TKey key = (TKey) entry.getColumnValue(strIndexColumn);
            insert(key, new Pair(clusteringKey, pageInstance.fnGetPageName()));
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
        Vector<Pair> curValue = delete(key, strClusteringKeyValue);
        for (Pair pair : curValue) {
            if (strClusteringKeyValue.equals(pair.getCmpClusteringKey())) {
                insert(key, new Pair(strClusteringKeyValue, strNewPage));
                return;
            }
        }
    }

    public void update(TKey key, Comparable clusteringKey, TKey newKey) {
        String strPageName = search(key, clusteringKey);
        if (strPageName == null) return;
        delete(key, clusteringKey);
        insert(newKey, new Pair(clusteringKey, strPageName));
    }

    public Vector<Pair> delete(TKey key, Comparable strClusteringKeyValue) {
        Vector<Pair> curValue = btree.search(key);
        if (curValue == null) return null;
        if (strClusteringKeyValue == null) {
             btree.delete(key);
             return curValue;
        }
        Vector<Pair> toBeInserted = new Vector<>();
        Vector<Pair> toBeDeleted = new Vector<>();
        toBeInserted.addAll(curValue);
        for (Pair pair : curValue) {
            if (pair.getCmpClusteringKey().equals(strClusteringKeyValue)) {
                toBeDeleted.add(pair);
                toBeInserted.remove(pair);
                btree.delete(key);
                btree.insert(key, toBeInserted);
                return toBeDeleted;
            }
        }
        return null;
    }

}


class Pair implements Serializable {
    private Comparable cmpClusteringKey;
    private String strPageName;

    Pair(Comparable cmpClusteringKey, String strPageNam) {
        this.cmpClusteringKey = cmpClusteringKey;
        this.strPageName = strPageNam;
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
