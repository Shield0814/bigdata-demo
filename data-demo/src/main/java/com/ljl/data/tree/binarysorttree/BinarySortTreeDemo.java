package com.ljl.data.tree.binarysorttree;

public class BinarySortTreeDemo {

    public static void main(String[] args) {
        int[] arr = new int[]{2, 1, 4, 5, -1, 19, 81, 6, 3};
        BinarySortTree binarySortTree = new BinarySortTree();
        for (int i = 0; i < arr.length; i++) {
            binarySortTree.addNode(new Node(arr[i]));
        }
        binarySortTree.inOrderTraversal();
    }
}
