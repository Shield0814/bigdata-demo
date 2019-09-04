package com.ljl.hadoop.common.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LongSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable valueOut = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }
        valueOut.set(sum);
        context.write(key, valueOut);
    }
}
