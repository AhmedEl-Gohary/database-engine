package com.db;

import com.btree.*;

import java.io.Serializable;
import java.util.Vector;
/**
 * Represents an index for a database table using a B-tree structure.
 * @param <TKey> The type of the index key, must be Comparable.
 */
public class Index<TKey extends Comparable<TKey>> implements Serializable {
    /** The B-tree instance used for indexing. */
    BTree<TKey, Vector<Pair>> btree;
    /** Name of the index. */
    String strIndexName;
    /** Name of the table associated with this index. */
    String strTableName;
    /** Name of the column used for indexing. */
    String strIndexColumn;
    /** Constructs an empty Index. */
    public Index() {}

    /**
     * Constructs an Index with the specified details and builds the index.
     * @param strIndexName Name of the index.
     * @param strTableName Name of the associated table.
     * @param strIndexColumn Name of the column used for indexing.
     */
    public Index(String strIndexName, String strTableName, String strIndexColumn) {
        this.strIndexName = strIndexName;
        this.strTableName = strTableName;
        this.strIndexColumn = strIndexColumn;
        btree = new BTree<>();
        build();
    }
    /** Builds the index by adding existing pages from the associated table. */
    private void build() {
        Table tableInstance = (Table) DBApp.deserialize(strTableName);
        for (String page : tableInstance.vecPages) {
            addPage(page);
        }
    }
    /**
     * Adds tuples from a page to the index.
     * @param strPageName Name of the page.
     */
    private void addPage(String strPageName) {
        Page pageInstance = (Page) DBApp.deserialize(strPageName);
        for (Entry entry : pageInstance.vecTuples) {
            Comparable clusteringKey = (Comparable) entry.getClusteringKeyValue();
            TKey key = (TKey) entry.getColumnValue(strIndexColumn);
            insert(key, new Pair(clusteringKey, pageInstance.fnGetPageName()));
        }
    }
    /**
     * Inserts a key-value pair into the index.
     * @param key The key.
     * @param value The value pair.
     */
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
    /**
     * Searches for a key in the index and returns associated values.
     * @param key The key to search for.
     * @return A vector of pairs associated with the key, or null if not found.
     */
    public Vector<Pair> search(TKey key){
        Vector<Pair> targetValue = btree.search(key);
        if (targetValue == null) return null;
        Vector<Pair> result = new Vector<>();
        result.addAll(targetValue);
        return result;
    }
    /**
     * Finds the minimum value in the index.
     * @return A vector of pairs representing the minimum value in the index.
     */
    public Vector<Pair> findMinInIndex(){
        return this.btree.findMinInTree();
    }
    /**
     * Finds the maximum value in the index.
     * @return A vector of pairs representing the maximum value in the index.
     */
    public Vector<Pair> findMaxInIndex(){
        return this.btree.findMaxInTree();
    }
    /**
     * Finds values greater than a given key in the index.
     * @param key The key to compare against.
     * @return A vector of pairs greater than the given key.
     */
    public Vector<Pair> findGreaterThanKey(TKey key){
        Vector<Pair> ans =new Vector<>();
        BTreeLeafNode<TKey,Vector<Pair>> treeNode = btree.findMaxInTreeNode();
        l:while(treeNode != null ){
            for(int i=treeNode.getKeyCount()-1; i > -1; i--) {
                if(treeNode.getKey(i).compareTo(key)>0){
                    ans.addAll(treeNode.getValue(i));
                }
                else break l;
            }
            treeNode = (BTreeLeafNode<TKey, Vector<Pair>>) treeNode.getLeftSibling();
        }
        return ans;

    }
    /**
     * Finds values greater than or equal to a given key in the index.
     * @param key The key to compare against.
     * @return A vector of pairs greater than or equal to the given key.
     */
    public Vector<Pair> findGreaterThanOrEqualKey(TKey key){

        Vector<Pair> ans =new Vector<>();
        BTreeLeafNode<TKey,Vector<Pair>> treeNode = btree.findMaxInTreeNode();
        l:while(treeNode != null ){
            for(int i=treeNode.getKeyCount()-1;i>-1;i--){
                if(treeNode.getKey(i).compareTo(key)>=0){
                    ans.addAll(treeNode.getValue(i));
                }
                else break l;
            }
            treeNode = (BTreeLeafNode<TKey, Vector<Pair>>) treeNode.getLeftSibling();
        }
        return ans;
    }
    /**
     * Finds values less than a given key in the index.
     * @param key The key to compare against.
     * @return A vector of pairs less than the given key.
     */
    public Vector<Pair> findLessThanKey(TKey key){
        Vector<Pair> ans =new Vector<>();

        BTreeLeafNode<TKey,Vector<Pair>> treeNode = btree.findMinInTreeNode();
        l:while(treeNode != null ){
            for(int i=0;i<treeNode.getKeyCount();i++){
                if(treeNode.getKey(i).compareTo(key)<0){
                    ans.addAll(treeNode.getValue(i));
                }
                else break l;
            }
            treeNode = (BTreeLeafNode<TKey, Vector<Pair>>) treeNode.getRightSibling();
        }
        return ans;
    }
    /**
     * Finds values less than or equal to a given key in the index.
     * @param key The key to compare against.
     * @return A vector of pairs less than or equal to the given key.
     */
    public Vector<Pair> findLessThanOrEqualKey(TKey key){
        Vector<Pair> ans =new Vector<>();

        BTreeLeafNode<TKey,Vector<Pair>> treeNode = btree.findMinInTreeNode();
        l:while(treeNode != null ){
            for(int i=0;i<treeNode.getKeyCount();i++){
                if(treeNode.getKey(i).compareTo(key)<=0){
                    ans.addAll(treeNode.getValue(i));
                }
                else break l;
            }
            treeNode = (BTreeLeafNode<TKey, Vector<Pair>>) treeNode.getRightSibling();
        }
        return ans;
    }

    /**
     * Searches for a key and a corresponding clustering key in the index.
     * @param key The key to search for.
     * @param strClusteringKeyValue The clustering key to search for.
     * @return The name of the page associated with the given keys, or null if not found.
     */
    public String search(TKey key, Comparable strClusteringKeyValue){
        Vector<Pair> targetValue = btree.search(key);
        if (targetValue == null) return null;
        for (Pair pair: targetValue){
            if (pair.getCmpClusteringKey().equals(strClusteringKeyValue))
                return pair.getStrPageName();
        }
        return null;
    }
    /**
     * Updates the page associated with a key and clustering key in the index.
     * @param key The key to search for.
     * @param strClusteringKeyValue The clustering key to search for.
     * @param strNewPage The new page to associate with the keys.
     */
    public void updatePage(TKey key, Comparable strClusteringKeyValue, String strNewPage) {
        Vector<Pair> curValue = delete(key, strClusteringKeyValue);
        if (curValue == null){
            insert(key, new Pair(strClusteringKeyValue, strNewPage));
            return;
        }
        for (Pair pair : curValue) {
            if (strClusteringKeyValue.equals(pair.getCmpClusteringKey())) {
                insert(key, new Pair(strClusteringKeyValue, strNewPage));
                return;
            }
        }
    }
    /**
     * Updates a key in the index.
     * @param key The key to search for.
     * @param clusteringKey The clustering key to search for.
     * @param newKey The new key to update to.
     */
    public void update(TKey key, Comparable clusteringKey, TKey newKey) {
        String strPageName = search(key, clusteringKey);
        if (strPageName == null) return;
        delete(key, clusteringKey);
        insert(newKey, new Pair(clusteringKey, strPageName));
    }
    /**
     * Deletes a key-value pair from the index.
     * @param key The key.
     * @param strClusteringKeyValue The clustering key.
     * @return A vector of pairs representing the deleted key-value pair, or null if not found.
     */
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
                if (!toBeInserted.isEmpty())
                    btree.insert(key, toBeInserted);
                return toBeDeleted;
            }
        }
        return null;
    }
    /**
     * Returns a string representation of the index's B-tree structure.
     * @return The string representation of the B-tree structure.
     */
    @Override
    public String toString(){
        return btree.getTreeStructure();
    }

}

/**
 * Represents a pair of a clustering key and page name.
 */
class Pair implements Serializable {
    /** The clustering key. */
    private Comparable cmpClusteringKey;
    /** The name of the page. */
    private String strPageName;
    /**
     * Constructs a Pair with the specified clustering key and page name.
     * @param cmpClusteringKey The clustering key.
     * @param strPageNam The name of the page.
     */
    Pair(Comparable cmpClusteringKey, String strPageNam) {
        this.cmpClusteringKey = cmpClusteringKey;
        this.strPageName = strPageNam;
    }
    /**
     * Returns the clustering key of the pair.
     * @return The clustering key.
     */
    public Comparable getCmpClusteringKey() {
        return cmpClusteringKey;
    }
    /**
     * Sets the clustering key of the pair.
     * @param cmpClusteringKey The clustering key to set.
     */
    public void setCmpClusteringKey(Comparable cmpClusteringKey) {
        this.cmpClusteringKey = cmpClusteringKey;
    }

    /**
     * Returns the page name of the pair.
     * @return The page name.
     */
    public String getStrPageName() {
        return strPageName;
    }
    /**
     * Sets the page name of the pair.
     * @param strPageName The page name to set.
     */
    public void setStrPageName(String strPageName) {
        this.strPageName = strPageName;
    }
    /**
     * Returns a string representation of the Pair.
     * @return The string representation of the Pair.
     */
    @Override
    public String toString() {
        return cmpClusteringKey + " " + strPageName;
    }

}
