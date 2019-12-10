package com.ljl.flink.sensor.exercise

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TemperatureDiffAlertorApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.socketTextStream("bigdata116", 9999)
            .map {
                reading =>
                    val tmp = reading.split(",")
                    SensorReading(tmp(0), tmp(1).toLong, tmp(2).toDouble)
            }
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
                override def extractTimestamp(element: SensorReading): Long = element.timestamp
            })
            .keyBy(_.id)
            .timeWindow(Time.milliseconds(100))
            .process(new TemperDiffAlert)

        env.execute("TemperatureDiffAlertorApp")
    }

    class TemperDiffAlert extends ProcessWindowFunction[SensorReading, (SensorReading, SensorReading), String, TimeWindow] {

        override def process(key: String,
                             context: Context,
                             elements: Iterable[SensorReading],
                             out: Collector[(SensorReading, SensorReading)]): Unit = {
            val sortedReading = elements.toList.sortWith(_.timestamp < _.timestamp)
            sortedReading
        }
    }

}
