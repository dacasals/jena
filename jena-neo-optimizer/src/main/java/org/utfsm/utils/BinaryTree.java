package org.utfsm.utils;// Recursive Java program for level order traversal of Binary Tree

/* Class containing left and right child of current 
node and key value*/

import org.apache.jena.atlas.json.JsonArray;

import java.util.ArrayList;
import java.util.List;

public abstract class BinaryTree<T>
{
    protected static class Node<T>
    {
        public T data;
        public Node<T> left;
        public Node<T> right;
        public Node(T item)
        {
            data = item;
            left = right = null;
        }

    }
    // Root of the Binary Tree
    Node<T> root;
    public String delimiterValues;
    public BinaryTree()
    {
        root = null;
        delimiterValues = "ᶷ";
    }
    public BinaryTree(String delimiterValues)
    {
        root = null;
        this.delimiterValues = delimiterValues;
    }

    public abstract String printDataJoin(Node<T> node );
    public abstract String printLeafDataNode(Node<T> node);

    public JsonArray outputJson(Node<T> node){
        JsonArray json = new JsonArray();
        if(node.left == null && node.right == null) {
            json = new JsonArray();
            json.add(printLeafDataNode(node));
            return json;
        }

        String dataRoot = printDataJoin(node);

        json.add(dataRoot);
        json.add(outputJson(node.left));
        json.add(outputJson(node.right));
        return  json;
    }
    @Override
    public String toString() {
        return outputJson(root).toString();
    }


    public abstract void defineDataJoinNode(Node<T> node);

    public void addNodeList(List<T> arrayList) {
        int index = 0;
        if(arrayList.size()==1){
            root = new Node<>(arrayList.get(0));
        }
        else if(arrayList.size()>=2)
            while (index < arrayList.size()) {
                if(root == null){
                    Node<T> first = new Node<>(arrayList.get(index));
                    index++;
                    Node<T> second = new Node<>(arrayList.get(index));
                    Node<T> newRoot = new Node<>(null);
                    newRoot.left = first;
                    newRoot.right = second;
                    //Todo Modificar root con datos del join
                    defineDataJoinNode(newRoot);
                    root  = newRoot;
                }
                else{
                    T nextLeafOnRight = arrayList.get(index);
                    Node<T> newRoot = new Node<>(null);
                    newRoot.left = root;
                    newRoot.right = new Node<>(nextLeafOnRight);
                    //Todo Modificar valores del root en dependencia de sus hijos.
                    defineDataJoinNode(newRoot);
                    root = newRoot;
                }
                index++;
            }
    }
    /* Driver program to test above functions */
    public static void main(String args[])
    {
        BinaryTree<Integer> tree = new BinaryTree<Integer>() {
            @Override
            public void defineDataJoinNode(Node<Integer> node) {
                node.data = -1;
            }
            @Override
            public String printDataJoin(Node<Integer> node) {
                return node.data.toString();
            }
            @Override
            public String printLeafDataNode(Node<Integer> node) {
                return node.data.toString();
            }
        };
        ArrayList<Integer> lista = new ArrayList<>();
        lista.add(6);
        lista.add(7);
        lista.add(5);
        lista.add(3);
        lista.add(64);
        lista.add(445);
        tree.addNodeList(lista);
        System.out.println(tree);
    }
} 
