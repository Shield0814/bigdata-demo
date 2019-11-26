package com.ljl.flink.stream.window

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AllWindowedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object WindowFunctionAppScala {

    def main(args: Array[String]): Unit = {

        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        val data = senv.socketTextStream("bigdata116", 9999)

        val words = data.flatMap(_.split("\\W")).map((_, 1))

        // non-keyed window 不能并行处理
        val allWindowedStream: AllWindowedStream[(String, Int), TimeWindow] =
            words.timeWindowAll(Time.seconds(5))
        // keyed window 可以并行处理
        words.keyBy(0).setParallelism(1)


        //        aggregateFunctionBaseKeyedWindow(words,senv)

        //        foldFunctionBaseKeyedWindow(words,senv)


    }

    /**
      * 测试ProcessWindowFunction
      *
      * @param data
      * @param senv
      */
    def processWindowFunction(data: DataStream[(String, Int)], senv: StreamExecutionEnvironment): Unit = {
        //        data.keyBy(0)
        //            .timeWindow(Time.seconds(5))
        //            .process[Int](new CustomProcessWindowFunction)
    }

    /**
      * FoldFunction测试
      *
      * @param data
      * @param senv
      */
    def foldFunctionBaseKeyedWindow(data: DataStream[(String, Int)], senv: StreamExecutionEnvironment): Unit = {
        data.map(o => (o._1, o._1.length))
            .keyBy(0)
            //            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
            .fold(Int.MaxValue) {
                (acc, v) => if (acc > v._2) v._2 else acc
            }.print()
        senv.execute("foldFunctionBaseKeyedWindow")
    }

    /**
      * 测试自定义聚合函数AggregateFunction
      *
      * @param data
      * @param senv
      */
    def aggregateFunctionBaseKeyedWindow(data: DataStream[(String, Int)], senv: StreamExecutionEnvironment): Unit = {
        data.map(v => (v._1, (v._2 + math.random * 100).toInt))
            .keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .aggregate(new CustomAvgAggregateFunction)
            .print()
        senv.execute("aggregateFunctionBaseKeyedWindow")

    }

    /**
      * reduce function 测试
      *
      * @param data
      * @param senv
      */
    def reduceFunctionBaseKeyedWindow(data: DataStream[(String, Int)], senv: StreamExecutionEnvironment): Unit = {
        data.keyBy(0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
            .reduce((v1, v2) => (v1._1, v1._2 + v2._2))
            .print()
        senv.execute("reduceFunctionBaseKeyedWindow")
    }

    /**
      * key window 和 non-key window 比较测试
      * key window 可以并行执行，而 non-key window 不能并行执行
      *
      * @param data
      * @param senv
      */
    def keyedWindowVsNonKeyWindow(data: DataStream[(String, Int)], senv: StreamExecutionEnvironment): Unit = {

        data.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))


    }


}
