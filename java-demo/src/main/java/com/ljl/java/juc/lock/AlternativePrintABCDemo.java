package com.ljl.java.juc.lock;


import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class ShareResource {

    volatile AtomicReference<String> flag = new AtomicReference<>("A");
    Lock lock = new ReentrantLock();
    Condition conditionA = lock.newCondition();
    Condition conditionB = lock.newCondition();
    Condition conditionC = lock.newCondition();


    public void printA() {
        lock.lock();
        try {
            while (!"A".equals(flag.get())) {
                conditionA.await();
            }
            System.out.println("A");
            flag.compareAndSet("A", "B");
            conditionB.signal();
        } catch (InterruptedException e) {
            System.out.println("printA线程被中断");
        } finally {
            lock.unlock();
        }
    }

    public void printB() {
        lock.lock();
        try {
            while (!"B".equals(flag.get())) {
                conditionB.await();
            }
            System.out.println("B");
            flag.compareAndSet("B", "C");
            conditionC.signal();
        } catch (InterruptedException e) {
            System.out.println("printB线程被中断");
        } finally {
            lock.unlock();
        }
    }

    public void printC() {
        lock.lock();
        try {
            while (!"C".equals(flag.get())) {
                conditionC.await();
            }
            System.out.println("C");
            flag.compareAndSet("C", "A");
            conditionA.signal();
        } catch (InterruptedException e) {
            System.out.println("printC线程被中断");
        } finally {
            lock.unlock();
        }
    }
}


public class AlternativePrintABCDemo {
    public static void main(String[] args) {
        ShareResource res = new ShareResource();
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                res.printA();
            }
        }, "AAA").start();
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                res.printB();
            }
        }, "BBB").start();
        new Thread(() -> {
            for (int i = 0; i < 10; i++) {
                res.printC();
            }
        }, "CCC").start();
    }
}
