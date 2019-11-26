package com.ljl.flink.sensor.processfunction.windowfunction.aggregatefunction

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * AggregateFunction和ProcessWindowFunction配合使用计算每个传感器在某个窗口的平均温度
  */
class AvgTemperatureProcessWindowFunction
    extends ProcessWindowFunction[(String, Double), (String, Double, Long), String, TimeWindow] {

    /**
      *
      * @param key
      * @param context
      * @param elements 如果配合增量聚合函数，则每个key对应的元素将只有一个
      * @param out
      */
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double)],
                         out: Collector[(String, Double, Long)]): Unit = {
        var avgTemp = ("", 0.0)
        var count = 0
        val windowEnd = context.window.getEnd
        val iter = elements.iterator
        avgTemp = iter.next()
        out.collect(avgTemp._1, avgTemp._2, windowEnd)
    }
}
