package com.ljl.data.sortalgorithm;

public class InsertSort {


    public static void main(String[] args) {
        Integer[] arr = BubbleSort.generateData(10);
        long start = System.currentTimeMillis();
        insertSort(arr);
        System.out.println("耗时:" + (System.currentTimeMillis() - start) / 1000);
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + ",");
        }
    }


    /**
     * 插入排序
     *
     * @param arr
     * @param <T>
     */
    public static <T extends Comparable<T>> void insertSort(T[] arr) {
        for (int i = 1; i < arr.length; i++) {

            //有序表最后一个元素的下标
            int orderEndIdx = i - 1;
            //要插入的值
            T insertValue = arr[i];

            while (orderEndIdx >= 0 && insertValue.compareTo(arr[orderEndIdx]) < 0) {
                arr[orderEndIdx + 1] = arr[orderEndIdx];
                orderEndIdx--;
            }
            arr[orderEndIdx + 1] = insertValue;

        }

    }
}
