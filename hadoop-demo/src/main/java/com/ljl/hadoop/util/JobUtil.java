package com.ljl.hadoop.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobUtil {

    public static void runJob(String[] args, Tool tool) {
        //1. 解析应用参数
        String queue = ParameterUtil.getArgByKey(args, "queue");
        String numReduceTasks = ParameterUtil.getArgByKey(args, "num-reduce-task");
        //2. 配置mr相关参数，队列，reducer任务数
        Configuration conf = new Configuration();
        if (!StringUtils.isEmpty(queue)) {
            conf.set("mapreduce.job.queuename", queue);
        } else {
            conf.set("mapreduce.job.queuename", "default");
        }


        try {
            if (!StringUtils.isEmpty(numReduceTasks)) {
                conf.setInt("mapreduce.job.reduces", Integer.parseInt(numReduceTasks));
            }
        } catch (NumberFormatException e) {
            conf.setInt("mapreduce.job.reduces", 1);
        }

        //创建并运行任务
        tool.setConf(conf);
        try {
            ToolRunner.run(tool, args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
