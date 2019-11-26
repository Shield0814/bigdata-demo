package com.ljl.hadoop.mr.yutube;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text keyOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split("\t");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i < 9) {
                if (i == 3) {
                    String[] categories = fields[3].split("&");
                    StringBuilder sb1 = new StringBuilder(categories[0].trim());
                    for (int j = 1; j < categories.length; j++) {
                        sb1.append("&").append(categories[j].trim());
                    }
                    sb.append(sb1.toString()).append("\t");
                } else {
                    sb.append(fields[i]).append("\t");
                }
            } else {
                //好友信息用&分割
                sb.append(fields[i].trim()).append("&");
            }
        }

        keyOut.set(sb.substring(0, sb.length() - 1));

        context.write(keyOut, NullWritable.get());
    }
}
