package com.ljl.flink.sensor.processfunction.windowfunction.foldfunction

import com.ljl.flink.sensor.source.SensorSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SensorLowTempApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val readings = env.addSource(new SensorSource).setParallelism(1)

        readings.keyBy(_.id)
            .timeWindow(Time.seconds(5))
            .fold(("", 100.0), new LowTempFoldFunction)
            .print()

        env.execute("SensorLowTempApp")
    }
}
