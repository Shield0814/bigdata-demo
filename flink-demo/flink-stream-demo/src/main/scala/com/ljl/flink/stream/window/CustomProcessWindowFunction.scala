package com.ljl.flink.stream.window


import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 自定义ProcessWindowFunction
  */
class CustomProcessWindowFunction extends ProcessWindowFunction[(String, Int), Int, String, TimeWindow] {


    override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[Int]): Unit = {
        var count = 0
        for (element <- elements) {
            count += 1
        }
        print(context.currentProcessingTime + "," + context.currentWatermark + "," + context.window)
        out.collect(count)
    }
}
