package com.ljl.data.tree;

public class BinaryTreeDemo {

    public static void main(String[] args) {
        HeroNode root = new HeroNode("1", "宋江");
        HeroNode node1 = new HeroNode("2", "吴用");
        HeroNode node2 = new HeroNode("3", "卢俊义");
        HeroNode node3 = new HeroNode("4", "林冲");
        HeroNode node4 = new HeroNode("5", "关胜");
        root.setLeft(node1);
        root.setRight(node2);
        node2.setLeft(node4);
        node2.setRight(node3);
        BinaryTree binaryTree = new BinaryTree(root);
//        System.out.println("前序遍历:");
//        //1,2,3,5,4
//        binaryTree.preOrderTraversal();
//        System.out.println("中序遍历:");
//        //2,1,5,3,4
//        binaryTree.inOrderTraversal();
//        System.out.println("后序遍历:");
//        //2,5,4,3,1
//        binaryTree.postOrderTraversal();
//        System.out.println("========================前序查找:========================");
//        System.out.println(binaryTree.getWithPreOrderTraversal("11"));
//        System.out.println("========================中序查找:========================");
//        System.out.println(binaryTree.getWithInOrderTraversal("11"));
//        System.out.println("========================后序查找:========================");
//        System.out.println(binaryTree.getWithPostrderTraversal("11"));

        System.out.println("===============删除前：===============");

        binaryTree.preOrderTraversal();
        System.out.println("===============删除后：===============");
        binaryTree.deleteNode("3");
        binaryTree.preOrderTraversal();
    }
}
