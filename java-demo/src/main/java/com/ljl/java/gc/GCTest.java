package com.ljl.java.gc;

import java.lang.ref.SoftReference;

public class GCTest {


    public static void softReferenceWithNotEnough() {

        SoftReference<Object> o2 = new SoftReference<>(new Object());

        System.out.println(o2.get());
        System.out.println("===☣==============");
        try {
            byte[] bytes = new byte[100 * 1024 * 1024];
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println(o2.get());
        }
    }

    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, IllegalAccessException, InstantiationException {


//        Class<?> clazz2 = Class.forName("net.sf.cglib.proxy.Enhancer");
//
//
//        Object o = clazz2.newInstance();
//        System.out.println(o instanceof Enhancer);

    }
}
