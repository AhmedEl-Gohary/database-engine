package com.btree;


import java.util.Vector;

/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different,
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements java.io.Serializable {
    private BTreeNode<TKey> root;

    public BTree() {
        this.root = new BTreeLeafNode<TKey, TValue>();
    }

    public String getTreeStructure(TKey minKey){
        StringBuilder st = new StringBuilder();
        BTreeLeafNode<TKey, TValue> curNode = findLeafNodeShouldContainKey(minKey);
         while(curNode != null){
             st.append(curNode );
             curNode = (BTreeLeafNode<TKey, TValue>) curNode.rightSibling;
         }
         return st.toString();
    }
    /**
     * Insert a new key and its associated value into the B+ tree.
     */
    public void insert(TKey key, TValue value) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
        leaf.insertKey(key, value);

        if (leaf.isOverflow()) {
            BTreeNode<TKey> n = leaf.dealOverflow();
            if (n != null)
                this.root = n;
        }
    }

    /**
     * Search a key value on the tree and return its associated value.
     */
    public TValue search(TKey key) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        int index = leaf.search(key);
        return (index == -1) ? null : leaf.getValue(index);
    }

    /**
     * Delete a key and its associated value from the tree.
     */
    public void delete(TKey key) {
        BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

        if (leaf.delete(key) && leaf.isUnderflow()) {
            BTreeNode<TKey> n = leaf.dealUnderflow();
            if (n != null)
                this.root = n;
        }
    }


    /**
     * Search the leaf node which should contain the specified key
     */
    @SuppressWarnings("unchecked")
    public BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
        BTreeNode<TKey> node = this.root;
        while (node.getNodeType() == TreeNodeType.InnerNode) {
            node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
        }

        return (BTreeLeafNode<TKey, TValue>)node;
    }
    public TValue findMinInTree(){
        BTreeNode<TKey> node = this.root;
        if(root == null) return null;
        while (true) {
            if(node instanceof BTreeInnerNode<TKey>)
                node = ((BTreeInnerNode<TKey>)node).findLeftElement();
            else break;
        }

        return ((BTreeLeafNode<TKey,TValue>)node).getFirstValue();
    }
    public TValue findMaxInTree(){
        BTreeNode<TKey> node = this.root;
        if(root == null) return null;
        while (true) {
            if(node instanceof BTreeInnerNode<TKey>)
                node = ((BTreeInnerNode<TKey>)node).findRightElement();
            else break;
        }


        return ((BTreeLeafNode<TKey,TValue>)node).getLastValue();
    }

    public BTreeLeafNode<TKey,TValue> findMaxInTreeNode(){
        BTreeNode<TKey> node = this.root;
        while (true) {
            if(node instanceof BTreeInnerNode<TKey>)
                node = ((BTreeInnerNode<TKey>)node).findRightElement();
            else break;
        }


        return ((BTreeLeafNode<TKey,TValue>)node);
    }
    public BTreeLeafNode<TKey,TValue> findMinInTreeNode(){
        BTreeNode<TKey> node = this.root;
        if(root == null) return null;

        while (true) {
            if(node instanceof BTreeInnerNode<TKey>)
                node = ((BTreeInnerNode<TKey>)node).findLeftElement();
            else break;
        }

        return ((BTreeLeafNode<TKey,TValue>)node);
    }
    static class Tuple implements java.io.Serializable {
        Object[] record;

        public Tuple(int n) {
            record = new Object[n];
        }

        public Tuple(Object[] record) {
            this.record = new Object[record.length];
            for (int i = 0; i < record.length; i++) {
                this.record[i] = record[i];
            }
        }

        public String toString() {
            String s = "";
            for (Object o : record) s = s.concat(o.toString() + " ");
            return s;
        }
    }

    public static void main(String[] args) {

    }
}