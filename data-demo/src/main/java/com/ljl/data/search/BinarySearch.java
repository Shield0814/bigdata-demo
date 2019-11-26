package com.ljl.data.search;

public class BinarySearch {


    public static void main(String[] args) {
        Integer[] arr = {1, 2, 3, 4, 5, 6, 7};
//        System.out.println(binarySearch(arr, 1));
        System.out.println(binarySearchViaRecursive(arr, 7, 0, arr.length - 1));
    }

    /**
     * 二分查找
     *
     * @param arr   必须是有序数组
     * @param value 要查找的值
     * @param <T>
     * @return -1 标识没有找到
     */
    public static <T extends Comparable<T>> int binarySearch(T[] arr, T value) {

        int left = 0;
        int right = arr.length;

        while (left < right) {
            //找到中分位置
            int mid = (left + right) / 2;

            if (value.compareTo(arr[mid]) == 0) {
                //如果中间位置的值等于要查找的值，返回下标
                return mid;
            } else {
                if (value.compareTo(arr[mid]) > 0) {
                    //如果要查找的值大于中分位置值，则left设置 为mid
                    left = mid;
                } else {
                    ////如果要查找的值小于于中分位置值，则right设置 为mid
                    right = mid;
                }
            }
            //如果中分值 不等于 要查找的值，并且mid为0 或数组最后一个索引退出循环
            if (mid == arr.length - 1 || mid == 0) {
                break;
            }

        }
        return -1;
    }

    /**
     * 通过递归实现二分查找
     *
     * @param arr
     * @param value
     * @param <T>
     * @return
     */
    public static <T extends Comparable<T>> int binarySearchViaRecursive(T[] arr, T value, int l, int r) {

        if (l > r) {
            return -1;
        }
        int mid = (l + r) / 2;
        if (arr[mid].compareTo(value) == 0) {
            return mid;
        } else if (arr[mid].compareTo(value) > 0) {
            return binarySearchViaRecursive(arr, value, l, mid - 1);
        } else {
            return binarySearchViaRecursive(arr, value, mid + 1, r);
        }
    }


}
