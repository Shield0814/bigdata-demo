package com.ljl.data.search;

public class LinearSearch {

    /**
     * 线性查找一个值
     *
     * @param arr
     * @param value
     * @param <T>
     * @return
     */
    public <T extends Comparable<T>> int linearSearch(T[] arr, T value) {
        for (int i = 0; i < arr.length; i++) {
            if (value.compareTo(arr[i]) == 0) {
                return i;
            }
        }
        return -1;
    }

}
