package com.ljl.hadoop.common.key;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义MR key主要用于shuffle之后的数据排序，不shuffle的话是不会进行排序的
 */
public class FlowKey implements WritableComparable {

    private String phoneNo;

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
