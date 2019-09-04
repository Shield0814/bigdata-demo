package com.ljl.hadoop.mr.groupingcomparator;

import com.ljl.hadoop.common.key.OrderKey;
import com.ljl.hadoop.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class OrderDriver implements Tool {

    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(OrderKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(OrderReducer.class);
        job.setOutputKeyClass(OrderKey.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderSortComparator.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean res = job.waitForCompletion(true);
        return res ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    public static void main(String[] args) {
        OrderDriver driver = new OrderDriver();
        Configuration conf = new Configuration();
        driver.setConf(conf);
        JobUtil.runJob(args, driver);
    }
}
