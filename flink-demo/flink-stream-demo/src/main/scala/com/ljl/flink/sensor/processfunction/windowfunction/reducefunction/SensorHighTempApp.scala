package com.ljl.flink.sensor.processfunction.windowfunction.reducefunction

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SensorHighTempApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val readings = env.addSource(new SensorSource).setParallelism(1)

        //使用 reduceFunction 计算每个传感器在一个5s的滚动窗口中的最低温度
        readings.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
            override def extractTimestamp(element: SensorReading): Long = {
                element.timestamp
            }
        }).keyBy(_.id)
            .timeWindow(Time.seconds(5))
            .reduce(new HighTempReduceFunction)
            .print()

        env.execute("SensorHighTempApp ReduceFunction")
    }
}
