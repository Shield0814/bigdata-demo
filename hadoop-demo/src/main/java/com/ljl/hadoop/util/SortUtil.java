package com.ljl.hadoop.util;

import java.util.Comparator;

public class SortUtil {

    /**
     * 冒泡排序法
     *
     * @param arr        要排序的数组
     * @param comparator 比较器
     * @param ascend     是否升序
     * @param <T>        要排序数组中的元素类型
     */
    public static <T> void bubbleSort(T[] arr, Comparator<T> comparator, boolean ascend) {
        for (int i = 0; i < arr.length; i++) {
            for (int j = 0; j < arr.length - i - 1; j++) {
                if (comparator.compare(arr[j], arr[j + 1]) > 0) {
                    T tmp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = tmp;
                }
            }
        }
    }

    public static <T> void quickSort(T[] arr, int left, int right, Comparator<T> comparator) {
        int mid = (left + right) / 2;
        T privot = arr[mid];
        int l = left;
        int r = right;

        while (l <= mid && r >= mid) {
            int m = l;
            int n = r;
            for (int i = 0; i <= mid; i++) {
                if (comparator.compare(arr[l], privot) > 0) {
                    m = i;
                    break;
                }
            }
            for (int i = r; i > mid; i--) {
                if (comparator.compare(arr[r], privot) < 0) {
                    n = i;
                    break;
                }
            }
            if (m != n) {
                T tmp = arr[m];
                arr[m] = arr[n];
                arr[n] = tmp;
            }

        }


    }


}
