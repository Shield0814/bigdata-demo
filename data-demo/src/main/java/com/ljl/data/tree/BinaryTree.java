package com.ljl.data.tree;

public class BinaryTree {

    //二叉树的根节点
    private HeroNode root;


    public void preOrderTraversal() {
        if (root != null) {
            root.preOrderTraversal();
        } else {
            System.out.println("二叉树未初始化");
        }
    }

    public void deleteNode(String id) {
        if (root != null) {
            if (root.getNo().equals(id)) {
                root = null;
                return;
            } else {
                root.deleteNode(id);
            }
        } else {
            System.out.println("二叉树未初始化,不能删除");
        }
    }

    /**
     * 前序查找
     *
     * @param id
     * @return
     */
    public HeroNode getWithPreOrderTraversal(String id) {
        if (root != null) {
            return root.getWithPreOrderTraversal(id);
        } else {
            System.out.println("二叉树未初始化");
        }
        return null;
    }


    public void inOrderTraversal() {
        if (root != null) {
            root.inOrderTraversal();
        } else {
            System.out.println("二叉树未初始化");
        }
    }

    /**
     * 中序查找
     *
     * @param id
     * @return
     */
    public HeroNode getWithInOrderTraversal(String id) {
        if (root != null) {
            return root.getWithInOrderTraversal(id);
        } else {
            System.out.println("二叉树未初始化");
        }
        return null;
    }

    public void postOrderTraversal() {
        if (root != null) {
            root.postOrderTraversal();
        } else {
            System.out.println("二叉树未初始化");
        }
    }

    /**
     * 后序查找
     *
     * @param id
     * @return
     */
    public HeroNode getWithPostrderTraversal(String id) {
        if (root != null) {
            return root.getWithPostOrderTraversal(id);
        } else {
            System.out.println("二叉树未初始化");
        }
        return null;
    }

    public BinaryTree() {
    }

    public HeroNode getRoot() {
        return root;
    }

    public void setRoot(HeroNode root) {
        this.root = root;
    }

    public BinaryTree(HeroNode root) {
        this.root = root;
    }
}

