package com.ljl.hadoop.common.value;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    @Override
    public String toString() {
        Writable[] values = get();
        StringBuilder sb = new StringBuilder();
        for (Writable value : values) {
            sb.append(value.toString().concat(","));

        }
        return sb.substring(0, sb.lastIndexOf(","));
    }
}
