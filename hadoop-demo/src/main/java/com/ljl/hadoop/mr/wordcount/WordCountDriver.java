package com.ljl.hadoop.mr.wordcount;

import com.ljl.hadoop.util.JobUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountDriver implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] args) throws Exception {
        //1.创建作业
        Job job = Job.getInstance(getConf());

        //2. 设置jar
        job.setJarByClass(WordCountDriver.class);

        //3. 设置map相关参数
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //4.设置reducer相关参数
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //5.设置输入输出格式及相关参数
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, true);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setCombinerClass(WordCountReducer.class);


        //6.提交任务
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
        JobUtil.runJob(args, new WordCountDriver());
    }
}
