package com.ljl.hadoop.mr.groupingcomparator;

import com.ljl.hadoop.common.key.OrderKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReducer extends Reducer<OrderKey, NullWritable, OrderKey, NullWritable> {
    @Override
    protected void reduce(OrderKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
