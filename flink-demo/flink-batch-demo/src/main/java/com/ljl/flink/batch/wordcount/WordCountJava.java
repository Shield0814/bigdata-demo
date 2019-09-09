package com.ljl.flink.batch.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class WordCountJava {

    public static void main(String[] args) {

        // 获取运行时环境，类似spark core中的SparkContext
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        //args[0] = "file:///d:/data/access_log.20060831.decode.filter"
        DataSource<String> lines = env.readTextFile("file:///d:/data/emp.txt");

        //对数据进行切分，并计算词频
        AggregateOperator<Tuple2<String, Integer>> wordCounts = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split("\\W");
                for (int i = 0; i < words.length; i++) {
                    collector.collect(new Tuple2<>(words[i], 1));
                }
            }
        }).groupBy(0).sum(1);

        //输出数据
        try {
            wordCounts.setParallelism(1)
                    .writeAsText("file:///d:/data/out/3.txt");
            env.execute("java batch word count");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
