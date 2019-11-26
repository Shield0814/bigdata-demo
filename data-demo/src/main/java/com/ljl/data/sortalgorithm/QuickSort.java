package com.ljl.data.sortalgorithm;

public class QuickSort {


    public static void main(String[] args) {
        Integer[] arr = new Integer[]{21, 2, 1, 5, 23, 67, 13, 9, 25};
        int[] arr1 = new int[]{-9, 78, 0, 23, -567, 70};

        quickSort(arr, 0, 8);
//        quickSort(0,5,arr1);
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i] + ",");
        }
    }

    /**
     * 快速排序
     *
     * @param arr
     * @param <T>
     */
    public static <T extends Comparable<T>> void quickSort(T[] arr, int left, int right) {

        //1. 如果left大于等于right则排序[递归]结束
        if (left >= right) {
            return;
        }

        //2. 随便找个数做为基准值,这里取中间值
        int mid = (right + left) / 2;
        T pivot = arr[mid];

        //3. 以基准值为界,把比基准值大的放在一(右)边，把比基准值小的放在另一（左）边，

        //在基准值左边找一个比基准值大的数字索引
        int l = left;
        int r = right;
        //临时变量，交换时使用
        T tmp = null;
        while (l < r) {
            //从左向右找到第一个大于基准值的索引值l
            while (arr[l].compareTo(pivot) < 0) {
                l += 1;
            }
            //从右向左找到第一个小于基准值的索引值r
            while (arr[r].compareTo(pivot) > 0) {
                r -= 1;
            }
            //如果l>=r,说明基准值左边所有值都小于基准值，右边所有值都大于基准值，则退出循环
            if (l >= r) {
                break;
            }

            //从左边开始找到的大于基准值的值arr[l]和右边找到的小于基准值的值arr[r]交换
            tmp = arr[l];
            arr[l] = arr[r];
            arr[r] = tmp;

            //如果交换完成后arr[l]=pivot,则r-1,即 前移
            if (arr[l].compareTo(pivot) == 0) {
                r -= 1;
            }

            //如果交换后arr[r]=pivot，则 l+1，即后移
            if (arr[r].compareTo(pivot) == 0) {
                l += 1;
            }
        }


        if (l == r) {
            l += 1;
            r -= 1;
        }
        if (left < r) {
            quickSort(arr, left, r);
        }
        if (right > l) {
            quickSort(arr, l, right);
        }
    }


    public static void quickSort(int left, int right, int[] arr) {
        int l = left;
        int r = right;
        int pivot = arr[(left + right) / 2];
        int temp = 0;
        while (l < r) {
            while (arr[l] < pivot) {
                l += 1;
            }
            while (arr[r] > pivot) {
                r -= 1;
            }
            if (l >= r) {
                break;
            }
            temp = arr[l];
            arr[l] = arr[r];
            arr[r] = temp;
            if (arr[l] == pivot) {
                r -= 1;
            }
            if (arr[r] == pivot) {
                l += 1;
            }
        }
        if (l == r) {
            l += 1;
            r -= 1;
        }
        if (left < r) {
            quickSort(left, r, arr);
        }
        if (right > l) {
            quickSort(l, right, arr);
        }
    }

}
