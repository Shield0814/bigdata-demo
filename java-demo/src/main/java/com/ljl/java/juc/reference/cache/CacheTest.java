package com.ljl.java.juc.reference.cache;

public class CacheTest {

    public static void main(String[] args) throws InterruptedException {
        EmployeeCache cache = EmployeeCache.getInstance();
        for (int i = 0; i < 60000; i++) {
            cache.getEmployee(String.valueOf(i));
        }
    }

}  