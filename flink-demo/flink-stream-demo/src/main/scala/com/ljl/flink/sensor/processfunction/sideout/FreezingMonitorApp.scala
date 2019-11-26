package com.ljl.flink.sensor.processfunction.sideout

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object FreezingMonitorApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val sensorData = env.addSource(new SensorSource).setParallelism(1)
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(100)) {
                    override def extractTimestamp(element: SensorReading): Long = {
                        element.timestamp
                    }
                })

        val withFreezingData = sensorData.process(new FreezingMonitorProcessFunction)

        //输出冰冻警告
        withFreezingData.getSideOutput(new OutputTag[(String, SensorReading)]("freezing-alarms"))
            .print().setParallelism(1)

        //输出迟到元素
        withFreezingData.getSideOutput(new OutputTag[SensorReading]("late-record"))
            .print().setParallelism(1)

        env.execute("FreezingMonitorApp")
    }
}
