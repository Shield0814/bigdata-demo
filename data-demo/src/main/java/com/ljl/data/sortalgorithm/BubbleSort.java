package com.ljl.data.sortalgorithm;

import java.util.Random;

public class BubbleSort {

    public static void main(String[] args) {
        Integer[] arr = generateData(100);
        long start = System.currentTimeMillis();
        bubbleSort(arr);
        System.out.println("耗时:" + (System.currentTimeMillis() - start) / 1000);
//        for (int i = 0; i < arr.length; i++) {
//            System.out.print(arr[i] + ",");
//        }
    }

    /**
     * 冒泡排序
     *
     * @param arr
     * @param <T>
     */
    public static <T extends Comparable<T>> void bubbleSort(T[] arr) {
        T tmp;
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr.length - i - 1; j++) {
                if (arr[j].compareTo(arr[j + 1]) > 0) {
                    tmp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = tmp;
                }
            }
        }
    }


    public static Integer[] generateData(int len) {
        Random random = new Random();
        Integer[] data = new Integer[len];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt(3 * len);
        }
        return data;
    }

    public static int[] generateData2(int len) {
        Random random = new Random();
        int[] data = new int[len];
        for (int i = 0; i < data.length; i++) {
            data[i] = random.nextInt(3 * len);
        }
        return data;
    }
}
