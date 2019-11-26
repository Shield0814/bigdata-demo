package com.ljl.flink.sensor.windowassigner

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object ThirtySeconsWindowApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val unit: WindowedStream[SensorReading, String, TimeWindow] = env.addSource(new SensorSource)
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
                override def extractTimestamp(element: SensorReading): Long = element.timestamp
            }).keyBy(_.id)
            .window(new ThirtySecondsWindow)
        unit.max("temperature").print()

        env.execute("ThirtySeconsWindowApp")
    }
}
