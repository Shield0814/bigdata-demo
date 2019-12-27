package com.ljl.data.exercise;

public class DropEgg {


    public static void main(String[] args) {

        System.out.println(dropEgg(100));
    }

    public static int dropEgg(int n) {
        //假设在第k层丢会碎
        int k = 30;
        boolean[] arr = new boolean[n];
        for (int i = k - 1; i < n; i++) {
            arr[i] = true;
        }

        int count = 0;
        int left = 0;
        int right = 1;
        int mid = 0;
        while (true) {
            mid = (left + right) / 2;
            if (arr[mid]) {
                count += 1;
                if (!arr[mid - 1]) {
                    count += 1;
                    System.out.println("k在第" + mid + "层");
                    break;
                } else {
                    right = mid - 2;
                }
            } else {
                left = mid + 1;
            }
        }

        return count;
    }
}
