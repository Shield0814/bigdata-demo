package com.ljl.hadoop.mr.inputformat;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class JdbcMapper extends Mapper<Text, MapWritable, Text, Text> {

    private Text outValue = new Text();

    @Override
    protected void map(Text key, MapWritable value, Context context) throws IOException, InterruptedException {

        Set<Map.Entry<Writable, Writable>> entries = value.entrySet();
        StringBuilder sb = new StringBuilder();
        entries.forEach(entry -> {
            Text val = (Text) entry.getValue();
            sb.append(val.toString() + ",");
        });
        outValue.set(sb.toString());
        context.write(key, outValue);

    }
}
