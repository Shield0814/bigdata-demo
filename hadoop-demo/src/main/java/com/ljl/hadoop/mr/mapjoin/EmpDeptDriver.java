package com.ljl.hadoop.mr.mapjoin;


import com.ljl.hadoop.common.key.EmpDeptWritableComparable;
import com.ljl.hadoop.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.net.URI;

;

/**
 * mapjoin案例
 */
public class EmpDeptDriver implements Tool {

    private Configuration conf;

    public static void main(String[] args) {
        JobUtil.runJob(args, new EmpDeptDriver());
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(EmpDeptDriver.class);

        job.setMapperClass(EmpDeptMapper.class);
        job.setMapOutputKeyClass(EmpDeptWritableComparable.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setCacheFiles(new URI[]{URI.create("file:///d:/data/dept.txt")});

        job.setNumReduceTasks(0);
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
}
