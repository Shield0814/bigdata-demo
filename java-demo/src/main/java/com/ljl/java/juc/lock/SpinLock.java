package com.ljl.java.juc.lock;

import java.util.concurrent.atomic.AtomicReference;

public class SpinLock {

    AtomicReference<Thread> atomicReference = new AtomicReference<>();

    public void acquire() {
        do {
        } while (!atomicReference.compareAndSet(null, Thread.currentThread()));
    }

    public void release() {
        atomicReference.compareAndSet(Thread.currentThread(), null);
    }
}

