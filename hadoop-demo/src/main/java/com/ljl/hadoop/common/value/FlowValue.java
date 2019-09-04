package com.ljl.hadoop.common.value;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 数量数据
 */
public class FlowValue implements Writable {

    private int upFlow;
    private int downFlow;

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(upFlow);
        out.write(downFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
    }
}
