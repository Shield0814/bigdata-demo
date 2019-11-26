package com.ljl.data.search;

public class InsertValueSearch {

    public static void main(String[] args) {
        int[] arr = {1, 2, 3, 4, 5, 6, 7};
        System.out.println(insertValueSearch(arr, 7));
        System.out.println(insertValueSearchViaRecursive(arr, -1, 0, arr.length - 1));
    }

    /**
     * 插值查找
     *
     * @param arr
     * @param value
     * @return
     */
    public static int insertValueSearch(int[] arr, int value) {
        int left = 0;
        int right = arr.length - 1;
        int mid;
        while (left < right) {
            mid = left + (value - arr[left]) / (arr[right] - arr[left]) * (right - left);
            if (value == arr[mid]) {
                //如果中间位置的值等于要查找的值，返回下标
                return mid;
            } else {
                if (value > arr[mid]) {
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
     * 通过递归实现插值查找
     *
     * @param arr
     * @param value
     * @param left
     * @param right
     * @return
     */
    public static int insertValueSearchViaRecursive(int[] arr, int value, int left, int right) {

        //递归结束条件
        if (left > right || value < arr[left] || value > arr[right]) {
            return -1;
        }

        //找索引值
        int mid = left + (value - arr[left]) / (arr[right] - arr[left]) * (right - left);
        if (arr[mid] == value) {
            return mid;
        } else if (value > arr[mid]) {
            return insertValueSearchViaRecursive(arr, value, mid + 1, right);
        } else {
            return insertValueSearchViaRecursive(arr, value, left, mid - 1);
        }
    }
}
