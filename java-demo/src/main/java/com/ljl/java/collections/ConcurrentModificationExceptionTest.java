package com.ljl.java.collections;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ConcurrentModificationExceptionTest {

    public static void main(String[] args) throws InterruptedException {

//        testArrayList();


        testHashMap();
    }


    /**
     * hashmap 并发修改异常
     */
    public static void testHashMap() {
        HashMap<String, Integer> map1 = new HashMap<>();
        String lock = "ddd";
//        Map<String, Integer> map = Collections.synchronizedMap(map1);
        ConcurrentHashMap<String, Integer> map = new ConcurrentHashMap<>();
        for (int j = 0; j < 1000; j++) {
            new Thread(() -> {
                map.put("hello", map.getOrDefault("hello", 0) + 1);
            }, String.valueOf(j)).start();
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(map);
    }

    /**
     * arrayList并发修改异常
     */
    public static void testArrayList() {
        ArrayList<String> list = new ArrayList<>();
//        CopyOnWriteArrayList<String> list1 = new CopyOnWriteArrayList<>();
//        Vector<String> list1 = new Vector<>();
        List<String> list1 = Collections.synchronizedList(list);

        for (int j = 0; j < 30; j++) {
            new Thread(() -> {
                list1.add(UUID.randomUUID().toString().substring(0, 8));

            }).start();
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(list1.size());
    }
}
