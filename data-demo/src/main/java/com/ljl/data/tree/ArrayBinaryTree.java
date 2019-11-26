package com.ljl.data.tree;

/**
 * 顺序存储二叉树，用数组存储完全二叉数，完全二叉树的节点个数为：2^n-1，n为完全二叉数的深度
 * 1. 顺序二叉树通常只考虑完全二叉树
 * 2. 第n个元素的左子节点为  2 * n + 1
 * 3. 第n个元素的右子节点为  2 * n + 2
 * 4. 第n个元素的父节点为  (n-1) / 2
 */
public class ArrayBinaryTree {

    Object[] arr = {};

    public ArrayBinaryTree(Object[] arr) {
        this.arr = arr;
    }


    /**
     * 前序遍历
     *
     * @param rootIndex 根节点的在数组中的索引值
     */
    public void preOrderTraversal(int rootIndex) {
        //确保数组不为空
        if (arr == null && arr.length == 0) {
            System.out.println("树为空,不能遍历");
        }
        //输出当前节点
        System.out.println(arr[rootIndex]);
        //左递归输出所有节点
        if ((2 * rootIndex + 1) < arr.length) {
            preOrderTraversal(2 * rootIndex + 1);
        }

        //右递归输出所有节点
        if ((2 * rootIndex + 2) < arr.length) {
            preOrderTraversal(2 * rootIndex + 2);
        }
    }
}
