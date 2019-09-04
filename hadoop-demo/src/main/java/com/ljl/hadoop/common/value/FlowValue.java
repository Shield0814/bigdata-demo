package com.ljl.hadoop.common.value;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量数据，自定义hadoop MR的value
 * 注意关键点：
 *     write(DataOutput out)方法和 readFields(DataInput in)方法中
 *     一定要指定数据类型，并且write和read的顺序和类型必须一样，
 *     否则会报java.io.EOFException异常。
 * 错误方式：
 *
 *     public void write(DataOutput out) throws IOException {
 *         out.write(upFlow);   ***********************
 *         out.write(downFlow); ***********************
 *     }
 *
 *     public void readFields(DataInput in) throws IOException {
 *         this.upFlow = in.readInt();
 *         this.downFlow = in.readInt();
 *     }
 *
 * 正确方式：
 *
 *     public void write(DataOutput out) throws IOException {
 *         out.writeInt(upFlow);
 *         out.writeInt(downFlow);
 *     }
 *
 *     public void readFields(DataInput in) throws IOException {
 *         this.upFlow = in.readInt();
 *         this.downFlow = in.readInt();
 *     }
 *
 */
public class FlowValue implements Writable {

    private int upFlow;
    private int downFlow;


    //注意顺序和类型
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
    }

    //注意顺序和类型
    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
    }
}
