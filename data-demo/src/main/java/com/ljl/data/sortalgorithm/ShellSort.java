package com.ljl.data.sortalgorithm;

public class ShellSort {

    public static void main(String[] args) {

        Integer[] arr = BubbleSort.generateData(80000);
        long start = System.currentTimeMillis();
        shellSortViaMove(arr);
        System.out.println("耗时:" + (System.currentTimeMillis() - start) / 1000);
//        for (int i = 0; i < arr.length; i++) {
//            System.out.print(arr[i] + ",");
//        }
    }

    /**
     * 交换法实现shell排序
     *
     * @param arr
     * @param <T>
     */
    public static <T extends Comparable<T>> void shellSortViaSwap(T[] arr) {

        T tmp;
        for (int gap = arr.length / 2; gap > 0; gap /= 2) {
            for (int i = gap; i < arr.length; i++) {
                for (int j = i - gap; j >= 0; j -= gap) {
                    if (arr[j].compareTo(arr[j + gap]) > 0) {
                        tmp = arr[j];
                        arr[j] = arr[j + gap];
                        arr[j + gap] = tmp;
                    }
                }
            }
        }
    }

    /**
     * 移动法实现shell排序
     *
     * @param arr
     * @param <T>
     */
    public static <T extends Comparable<T>> void shellSortViaMove(T[] arr) {
        T tmp;
        for (int gap = arr.length / 2; gap > 0; gap /= 2) {
            for (int i = gap; i < arr.length; i++) {
                int j = i;
                tmp = arr[j];
                if (arr[j].compareTo(arr[j - gap]) < 0) {
                    while (j - gap >= 0 && tmp.compareTo(arr[j - gap]) < 0) {
                        arr[j] = arr[j - gap];
                        j -= gap;
                    }
                    arr[j] = tmp;
                }
            }
        }
    }
}

