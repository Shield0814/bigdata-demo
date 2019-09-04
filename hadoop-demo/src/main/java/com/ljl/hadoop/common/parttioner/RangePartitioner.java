package com.ljl.hadoop.common.parttioner;

import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

public class RangePartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text text, Text text2, int numPartitions) {
        return 0;
    }
}
