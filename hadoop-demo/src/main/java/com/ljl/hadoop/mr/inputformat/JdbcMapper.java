package com.ljl.hadoop.mr.inputformat;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JdbcMapper extends Mapper<Text, MapWritable, Text, MapWritable> {

    @Override
    protected void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
