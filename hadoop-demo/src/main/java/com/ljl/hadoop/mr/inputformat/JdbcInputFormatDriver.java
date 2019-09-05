package com.ljl.hadoop.mr.inputformat;

import com.ljl.hadoop.common.inputformat.JdbcInputFormat;
import com.ljl.hadoop.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import javax.xml.soap.Text;

public class JdbcInputFormatDriver implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);

        job.setJarByClass(JdbcInputFormatDriver.class);

        job.setMapperClass(JdbcMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(JdbcInputFormat.class);


        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        boolean res = job.waitForCompletion(true);
        return res ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {
        JdbcInputFormatDriver driver = new JdbcInputFormatDriver();
        Configuration conf = new Configuration();
        conf.set("xxx", "11111");
        conf.set("mapreduce.inputformat.jdbc.table", "tb_test");
        conf.setInt("mapreduce.job.reduces", 0);
        driver.setConf(conf);
        JobUtil.runJob(args, driver);
    }
}
