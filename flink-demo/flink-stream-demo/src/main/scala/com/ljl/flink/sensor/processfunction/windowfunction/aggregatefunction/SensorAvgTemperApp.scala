package com.ljl.flink.sensor.processfunction.windowfunction.aggregatefunction

import com.ljl.flink.sensor.source.SensorSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SensorAvgTemperApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val readings = env.addSource(new SensorSource).setParallelism(1)

        //方法1： 增量计算每个窗口内每个传感器的平均温度
        //        readings.keyBy(_.id)
        //                .timeWindow(Time.seconds(5),Time.seconds(5))
        //                .aggregate(new AvgTemperatureAggregateFunction)
        //                .print()


        //processWindowFunction和AggregateFunction配合使用提高效率
        //方法2：增量计算每个窗口内每个传感器的平均温度
        readings.keyBy(_.id)
            .timeWindow(Time.seconds(5), Time.seconds(5))
            .aggregate(new AvgTemperatureAggregateFunction, new AvgTemperatureProcessWindowFunction)
            .print()


        env.execute("SensorAvgTemperApp")
    }

}
