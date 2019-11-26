package com.ljl.data.tree.binarysorttree;

public class BinarySortTree {


    private Node root;

    //中序遍历
    public void inOrderTraversal() {
        if (root != null) {
            root.inOrderTraversal();
        } else {
            System.out.println("树为空");
        }
    }

    public void deleteNode(int value) {
        if (root == null) {
            System.out.println("树为空,不能删除");
        } else {
//            Node nodeToDelete = search(value);
//            if (nodeToDelete != null){
//                Node parent = searchParent(value);
//
//            }
        }
        return;
    }

    /**
     * 查找父节点
     *
     * @param value
     * @return
     */
    public Node searchParent(int value) {
        if (root == null) {
            return null;
        } else {
            return root.searchParent(value);
        }
    }

    /**
     * 查找节点
     *
     * @param value
     * @return
     */
    public Node search(int value) {
        if (root == null) {
            return null;
        } else {
            return root.searchNode(value);
        }
    }

    //添加节点
    public void addNode(Node node) {
        if (root == null) {
            root = node;
        } else {
            root.add(node);
        }
    }
}
