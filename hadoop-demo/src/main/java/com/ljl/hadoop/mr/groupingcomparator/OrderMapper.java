package com.ljl.hadoop.mr.groupingcomparator;

import com.ljl.hadoop.common.key.OrderKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderKey, NullWritable> {

    private OrderKey keyOut = new OrderKey();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split("\t");
        keyOut.setOrderId(splits[0]);
        keyOut.setPrice(Float.parseFloat(splits[2]));
        keyOut.setProdId(splits[1]);
        context.write(keyOut, NullWritable.get());
    }
}
