package com.ljl.data.tree.binarysorttree;

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
     * 中序遍历
     */
    public void inOrderTraversal() {
        if (this.left != null) {
            this.left.inOrderTraversal();
        }
        System.out.println(this);
        if (this.right != null) {
            this.right.inOrderTraversal();
        }
    }


    /**
     * 根据值查找节点
     *
     * @param value
     * @return
     */
    public Node searchNode(int value) {
        if (value == this.value) {
            return this;
        } else if (value < this.value) {
            if (this.left != null) {
                return this.left.searchNode(value);
            } else {
                return null;
            }
        } else {
            if (this.right != null) {
                return this.right.searchNode(value);
            } else {
                return null;
            }
        }
    }

    /**
     * 根据节点值在二叉排序树中查找该节点的父节点
     *
     * @param value
     * @return
     */
    public Node searchParent(int value) {
        if ((this.left != null && value == this.left.value) || (this.right != null && value == this.right.value)) {
            return this;
        } else if (this.left != null && value < this.left.value) {
            return this.left.searchParent(value);
        } else if (this.right != null && value >= this.right.value) {
            return this.right.searchParent(value);
        }
        return null;
    }

    /**
     * 向二叉排序数中添加节点
     *
     * @param node
     */
    public void add(Node node) {
        if (node.value < this.value) {
            //如果要插入的节点值小于当前节点值，在左子树插入
            if (this.left != null) {
                this.left.add(node);
            } else {
                this.left = node;
            }
        } else {
            //如果要插入的节点值大于等于当前节点值，在左子树插入
            if (this.right != null) {
                this.right.add(node);
            } else {
                this.right = node;
            }
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