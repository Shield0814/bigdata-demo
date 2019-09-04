package com.ljl.hadoop.common.parttioner;

import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

/**
 * 随机分区器
 *
 * @param <KEY>   mapper输出的key的类型
 * @param <VALUE> mapper输出的value的类型
 */
public class RandomPartitioner<KEY, VALUE> extends Partitioner<KEY, VALUE> {

    private Random random = new Random();

    @Override
    public int getPartition(KEY key, VALUE value, int numPartitions) {
        return random.nextInt(numPartitions);
    }
}
