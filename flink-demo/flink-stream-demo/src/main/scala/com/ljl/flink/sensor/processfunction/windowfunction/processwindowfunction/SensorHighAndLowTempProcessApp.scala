package com.ljl.flink.sensor.processfunction.windowfunction.processwindowfunction

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object SensorHighAndLowTempProcessApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val readings = env.addSource(new SensorSource).setParallelism(1)
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
                override def extractTimestamp(element: SensorReading): Long = element.timestamp
            })

        //5s的滚动窗口
        val window5s: WindowedStream[SensorReading, String, TimeWindow] = readings.keyBy(_.id)
            .timeWindow(Time.seconds(5))
            .sideOutputLateData(OutputTag[SensorReading]("late-record"))


        val highAndLowTemp = window5s.process(new HighAndLowTempProcessWindowFunction)
        highAndLowTemp.getSideOutput(OutputTag[SensorReading]("late-record"))
            .print()

        //        highAndLowTemp.print()


        env.execute("SensorHighAndLowTempProcessApp")
    }
}
