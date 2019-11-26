package com.ljl.flink.sensor.processfunction.coprocessfunction

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}

object SensorReadingPrintTimerApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val sensorData = env.addSource(new SensorSource()).setParallelism(1)

        val switchData = env.fromCollection(Array(
            ("sensor_2", 10000L),
            ("sensor_7", 20000L)
        ))
        val connectedStream: ConnectedStreams[SensorReading, (String, Long)] = sensorData.connect(switchData)
            .keyBy(_.id, _._1)
        connectedStream.process(new ReadingFilterCoProcessFunction)
            .print()

        env.execute("SensorReadingPrintTimerApp")
    }
}
