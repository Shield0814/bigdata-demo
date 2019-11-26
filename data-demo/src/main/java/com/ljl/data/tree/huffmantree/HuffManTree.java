package com.ljl.data.tree.huffmantree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HuffManTree {


    public static void main(String[] args) {
        int[] arr = new int[]{13, 7, 8, 3, 29, 6, 1};

        Node treeRoot = createHuffManTree(arr);
        if (treeRoot != null) {
            treeRoot.preOrderTraversal();
        } else {
            System.out.println("huffmantre 未创建");
        }
    }

    public static Node createHuffManTree(int[] arr) {
        //1. 为了操作方便把 arr 转化成一个 List<Node>
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < arr.length; i++) {
            nodes.add(new Node(arr[i]));
        }
        //2. 开始构建huffmantree
        while (nodes.size() > 1) {
            //2.1 对List中的元素进行排序
            Collections.sort(nodes);

            //2.2 取出根节点权值最小的两个二叉树节点
            //2.2.1 取出权值最小的节点
            Node left = nodes.get(0);
            //2.2.2 取出权值次小的节点
            Node right = nodes.get(1);

            //2.3 用取出的两个节点构建一个简单二叉树,权值为俩个个节点权值之和
            Node parent = new Node(left.value + right.value);
            parent.left = left;
            parent.right = right;

            //2.4 从List中移除已经处理的两个节点
            nodes.remove(left);
            nodes.remove(right);

            //2.5 将新构建的parent节点加入List<Node>中
            nodes.add(parent);
        }

        return nodes.get(0);

    }
}
