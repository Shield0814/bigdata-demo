package com.ljl.hbase.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class FruitDriver implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());

        Scan scan = new Scan();
        //对于mr任务务必记得关闭快缓存，否则会影响性能
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("fruit"),
                scan, FruitMapper.class,
                ImmutableBytesWritable.class, Put.class, job);


        job.setNumReduceTasks(1);

        //分区器设置成 HRegionPartitioner 的话，可以使用hbase的优化：
        //如果设置的reducer个数超过regions数量，reducer数量会被重置成regions数量,
        //!!!!设置reducer个数必须放在该操作前
        TableMapReduceUtil.initTableReducerJob("fruit_mr", FruitReducer.class, job, HRegionPartitioner.class);


        return 0;
    }


    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
