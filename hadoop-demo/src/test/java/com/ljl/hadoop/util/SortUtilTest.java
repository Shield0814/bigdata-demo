package com.ljl.hadoop.util;

import org.junit.Test;

public class SortUtilTest {

    @Test
    public void test1() {
        Integer[] arr = {2, 1, 9, 5, 2, 4, 32, 166};
        SortUtil.bubbleSort(arr, (o1, o2) -> Integer.compare(o1, o2), true);
        for (int i = 0; i < arr.length; i++) {
            System.out.println(arr[i]);
        }
    }

    @Test
    public void test2() {
        int[] arr = {2, 1121, 9, 5, 232, 4, 32, 166};
        qsort(arr, 0, arr.length - 1);
        for (int i = 0; i < arr.length; i++) {
            System.out.println(arr[i]);
        }
    }

    public int[] qsort(int arr[], int start, int end) {
        int pivot = arr[start];
        int i = start;
        int j = end;
        while (i < j) {
            while ((i < j) && (arr[j] > pivot)) {
                j--;
            }
            while ((i < j) && (arr[i] < pivot)) {
                i++;
            }
            if ((arr[i] == arr[j]) && (i < j)) {
                i++;
            } else {
                int temp = arr[i];
                arr[i] = arr[j];
                arr[j] = temp;
            }
        }
        if (i - 1 > start) arr = qsort(arr, start, i - 1);
        if (j + 1 < end) arr = qsort(arr, j + 1, end);
        return (arr);
    }
}
