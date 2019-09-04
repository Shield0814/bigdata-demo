package com.ljl.hadoop.mr.wordcount;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text keyOut = new Text();
    private LongWritable valueOut = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String valueStr = value.toString();
        if (!StringUtils.isEmpty(valueStr)) {
            String[] splits = valueStr.split("\\W");
            for (int i = 0; i < splits.length; i++) {
                keyOut.set(splits[i]);
                context.write(keyOut, valueOut);
            }
        }
    }
}
