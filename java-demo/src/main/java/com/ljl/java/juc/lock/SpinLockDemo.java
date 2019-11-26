package com.ljl.java.juc.lock;

public class SpinLockDemo {


    static class Resouce {

        int tiket = 100;
        SpinLock lock = new SpinLock();

        public void sale() {
            lock.acquire();
            System.out.println(Thread.currentThread().getId() + "买了票，余票" + --tiket);
            lock.release();
        }

    }


    public static void main(String[] args) {
        Resouce resouce = new Resouce();
        for (int i = 0; i < 100; i++) {
            new Thread(() -> {
                resouce.sale();
            }).start();
        }
    }
}
