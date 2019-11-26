package com.ljl.data.sortalgorithm;


import java.util.Arrays;

/**
 * 堆排序基础知识：
 * 1. 完全二叉数的顺序存储：
 * arr[i]的左子节点：arr[2*i+1];
 * arr[i]的右子节点：arr[2*i+2];
 * 二叉树中的第一个非叶子节点：arr[arr.length/2-1]
 * 2. 升序用大顶堆，降序用小顶堆
 */
public class HeapSort {

    public static void main(String[] args) {
        int[] arr = BubbleSort.generateData2(8000000);

        long start = System.currentTimeMillis();
        System.out.println();
        heapSort(arr);
        Arrays.sort(arr);
        long end = System.currentTimeMillis();
        System.out.println(end - start);
//        System.out.println(Arrays.toString(arr));
    }

    /**
     * 堆排序
     *
     * @param arr 要排序数组
     */
    public static void heapSort(int[] arr) {
        int tmp = 0;
        for (int i = arr.length / 2 - 1; i >= 0; i--) {
            adjustHeap(arr, i, arr.length);
        }

        for (int j = arr.length - 1; j > 0; j--) {
            tmp = arr[j];
            arr[j] = arr[0];
            arr[0] = tmp;
            adjustHeap(arr, 0, j);
        }
    }

    /**
     * 调整堆（二叉树）
     *
     * @param arr 带调整的数组
     * @param i   非叶子节点在数组中的索引
     * @param len 对多少个元素进行调整，len逐渐减少
     */
    public static void adjustHeap(int[] arr, int i, int len) {

        int temp = arr[i];

        for (int k = i * 2 + 1; k < len; k = k * 2 + 1) {

            if (k + 1 < len && arr[k] < arr[k + 1]) {
                k++;//k 指向右子节点
            }
            if (arr[k] > temp) {
                arr[i] = arr[k];//把较大值赋给较大节点
                i = k;//i指向k，继续循环比较
            } else {
                break;
            }
            arr[i] = temp;
        }
    }
}
