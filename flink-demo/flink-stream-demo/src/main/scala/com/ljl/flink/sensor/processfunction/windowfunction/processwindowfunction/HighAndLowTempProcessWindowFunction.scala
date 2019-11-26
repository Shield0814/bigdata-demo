package com.ljl.flink.sensor.processfunction.windowfunction.processwindowfunction

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class HighLowTemp(id: String, lowTemp: Double, highTemp: Double, windowStart: Long, windowEnd: Long)

/**
  * 计算5s的滚动窗口中的最低温度，最高温度，
  * 输出结果包含: (流的key,最低温度,最高温度,窗口开始时间，窗口关闭时间)
  */
class HighAndLowTempProcessWindowFunction
    extends ProcessWindowFunction[SensorReading, HighLowTemp, String, TimeWindow] {

    //高温
    var highTemp = Double.MinValue
    //低温
    var lowTemp = Double.MaxValue

    override def process(key: String,
                         context: Context,
                         elements: Iterable[SensorReading],
                         out: Collector[HighLowTemp]): Unit = {
        //计算本窗口中每个传感器的最低温度和最高温度

        elements.foreach(element => {
            if (element.temperature > highTemp) {
                highTemp = element.temperature
            }
            if (element.temperature < lowTemp) {
                lowTemp = element.temperature
            }
        })


        val windowStart = context.window.getStart
        val windowEnd = context.window.getEnd
        out.collect(HighLowTemp(key, lowTemp, highTemp, windowStart, windowEnd))
    }
}

