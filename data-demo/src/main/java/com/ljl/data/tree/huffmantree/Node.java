package com.ljl.data.tree.huffmantree;

class Node implements Comparable<Node> {

    //节点权值
    int value;
    //左节点
    Node left;
    //右节点
    Node right;

    public Node(int value) {
        this.value = value;
    }

    /**
     * 前序遍历
     */
    public void preOrderTraversal() {
        System.out.println(this);
        if (this.left != null) {
            this.left.preOrderTraversal();
        }
        if (this.right != null) {
            this.right.preOrderTraversal();
        }
    }

    @Override
    public int compareTo(Node o) {
        return this.value - o.value;
    }

    @Override
    public String toString() {
        return "Node{" +
                "value=" + value +
                '}';
    }
}