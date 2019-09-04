package com.ljl.hadoop.mr.flow;

import com.ljl.hadoop.common.value.IntArrayWritable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {

    private Text keyOut = new Text();
    private IntArrayWritable valueOut = new IntArrayWritable();
    private IntWritable up = new IntWritable(0);
    private IntWritable down = new IntWritable(0);
    private IntWritable total = new IntWritable(0);
    private IntWritable[] flows = new IntWritable[3];

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //数据格式：id\t手机号码\t网络ip\t访问url\t上行流量\t下行流量\t网络状态码
        //1.切分数据
        String[] splits = value.toString().split("\\t");

        //2. 处理数据,并写出到OutputCollector
        if (!StringUtils.isEmpty(splits[1])) {
            try {
                keyOut.set(splits[1] + "|" + 3);
                int downFlow = Integer.parseInt(splits[splits.length - 2]);
                int upFlow = Integer.parseInt(splits[splits.length - 3]);
                up.set(upFlow);
                down.set(downFlow);
                total.set(upFlow + downFlow);
                flows[0] = down;
                flows[1] = up;
                flows[2] = total;
                valueOut.set(flows);
                context.write(keyOut, valueOut);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }


    }
}
