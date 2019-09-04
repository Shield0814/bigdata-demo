package com.ljl.hadoop.common.reducer;

import com.ljl.hadoop.common.value.IntArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 按key同时计算多个字段的和
 * 实现逻辑：
 * mapper端把IntArrayWritable中的values的长度拼接在KEYOUT中，
 * reducer端解析KEYIN获得获得values的长度len，初始化结果长度并进行计算
 * 注意问题：
 * 1. Hadoop mr中 ArrayWritable不能直接使用，需要创建其子类，否则会报以下异常：
 * java.lang.RuntimeException: java.lang.NoSuchMethodException: org.apache.hadoop.io.ArrayWritable.<init>()
 * <p>
 * 2. Reducer端的values对应的迭代器只能单向遍历一次，第二次遍历的话会发现数据都为null
 * <p>
 * 3. Hadoop 序列化类不能强转，否则只会为空
 * Writable[] items = value.get();
 * IntWritable[] tmp = (Writable[]) items; //错误，tmp的值为空
 * <p>
 * 比如按手机号码聚合同时计算：上行流量，下行流量，总流量
 * select sum(up_flow) as up_flow,
 * sum(down_flow) as down_flow,
 * sum(up_flow + down_flow) as total_flow
 * from flow_data
 * group by phone_no
 */
public class IntArraySumReducer extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

    private Logger logger = Logger.getLogger(IntArraySumReducer.class);
    private Text keyOut = new Text();
    private IntArrayWritable valueOut = new IntArrayWritable();

    @Override
    protected void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        String[] splits = key.toString().split("|");
        int len = 5;
        try {
            if (splits.length != 2) {
                len = Integer.parseInt(splits[1]);
            } else {
                logger.warn("reducer 端还原原始key出错，这将导致聚合key的值发生改变，考虑重新设计key和len的分隔符");
            }
        } catch (NumberFormatException ne) {
            logger.error("reducer 端解析len发生错误，len将被设置成 15，这可能导致输出端有无效的0值");
        }
        IntWritable[] sums = new IntWritable[len];
        for (int i = 0; i < sums.length; i++) {
            sums[i] = new IntWritable(0);
        }
        for (IntArrayWritable value : values) {
            Writable[] items = value.get();
            for (int i = 0; i < items.length; i++) {
                sums[i].set(sums[i].get() + Integer.parseInt(items[i].toString()));
            }
        }
        keyOut.set(splits[0]);
        valueOut.set(sums);
        context.write(keyOut, valueOut);
    }
}
