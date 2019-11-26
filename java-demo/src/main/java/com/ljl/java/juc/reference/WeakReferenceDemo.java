package com.ljl.java.juc.reference;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

public class WeakReferenceDemo {

    public static void main(String[] args) throws InterruptedException {
        String wkStr = new String("weakReference");
        ReferenceQueue<String> wkrQueue = new ReferenceQueue<>();
        WeakReference<String> wkr = new WeakReference<>(wkStr, wkrQueue);

        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    Reference<? extends String> poll = wkrQueue.poll();
                    if (poll != null) {
                        System.out.println("虚引用对象被垃圾回收器收集");
                        break;
                    } else {
                        System.out.println("虚引用对象还未被回收");
                    }
                }
            }
        }).start();

        Thread.sleep(1000);

        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println(wkr.get());
                System.out.println("准备执行gc");
                System.gc();

            }
        }).start();
    }
}
