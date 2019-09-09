package com.ljl.flink.stream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SocketStreamWordCountJava {

    public static void main(String[] args) {

        //1. 解析命令行参数
        ParameterTool tool = ParameterTool.fromArgs(args);

        //2. 获得运行时环境
        StreamExecutionEnvironment jsenv = StreamExecutionEnvironment.getExecutionEnvironment();

        //3. 获得socket输入流
        DataStreamSource<String> lines;
        if (tool.has("host") && tool.has("port")) {
            lines = jsenv.socketTextStream(tool.get("host"), tool.getInt("port"));
        } else {
            throw new IllegalArgumentException("主机，端口号不能为空");
        }

        //4. 处理数据，切分单词,统计词频
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapWordCount = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\\W");
                for (int i = 0; i < words.length; i++) {
                    out.collect(new Tuple2<>(words[i], 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyByWordCount = flatMapWordCount.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWinWordCount = keyByWordCount.timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = timeWinWordCount.sum(1);

        wordCount.print().setParallelism(1);

        System.out.println(jsenv.getExecutionPlan());

        //触发任务执行，相当于spark的action操作
        try {
            jsenv.execute("java socket stream word count");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
