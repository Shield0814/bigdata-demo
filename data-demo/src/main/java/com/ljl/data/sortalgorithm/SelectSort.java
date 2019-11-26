package com.ljl.data.sortalgorithm;

public class SelectSort {


    public static void main(String[] args) {
        Integer[] arr = BubbleSort.generateData(80000);
        long start = System.currentTimeMillis();
        selectSort(arr);
        System.out.println("耗时:" + (System.currentTimeMillis() - start) / 1000);
    }

    /**
     * 选择排序
     * 思想：
     * 首先在未排序序列中找到最小（大）元素，存放到排序序列的起始位置，
     * 然后，再从剩余未排序元素中继续寻找最小（大）元素，
     * 然后放到已排序序列的末尾。以此类推，直到所有元素均排序完毕
     *
     * @param arr
     * @param <T>
     */
    public static <T extends Comparable<T>> void selectSort(T[] arr) {
        T tmp;
        for (int i = 0; i < arr.length - 1; i++) {
            //假设第i个值为最小值
            int min = i;
            //从第i+1个开始遍历数组，如果找到最小值索引
            for (int j = i + 1; j < arr.length; j++) {
                if (arr[j].compareTo(arr[min]) < 0) {
                    min = j;
                }
            }
            //如果最小值不是第i个值，第i个值和最小值交换
            if (min != i) {
                tmp = arr[min];
                arr[min] = arr[i];
                arr[i] = tmp;
            }

        }
    }

}
