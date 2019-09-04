package com.ljl.hadoop.util;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class WritableTest {


    @Test
    public void test1() {
        Writable[] items = new IntWritable[3];
        for (int i = 0; i < items.length; i++) {
            items[i] = new IntWritable(20);
        }
        IntWritable[] tmp = (IntWritable[]) items;
        for (int i = 0; i < tmp.length; i++) {
            System.out.println(tmp[i].get());
        }
    }

    @Test
    public void test() {
        Writable a = new IntWritable(1);
        LongWritable b = (LongWritable) a;
        System.out.println(b.get());
    }
}
