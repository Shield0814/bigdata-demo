package com.ljl.flink.sensor.processfunction.keyedbroadcastprocessfunction

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

case class ThresholdUpdate(id: String, threshold: Double)

/**
  *
  */
object UpdatableTempAlertApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val sensorData: DataStream[SensorReading] = env.addSource(new SensorSource)
        val thresholds: DataStream[ThresholdUpdate] = env.fromCollection(
            Array(
                ThresholdUpdate("sensor_1", 30.0)
            )
        )

        env.setStateBackend(new FsStateBackend(""))
        //默认的checkpoint模式是exactly_once
        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE)
        val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)
        // the descriptor of the broadcast state
        val broadcastStateDescriptor =
            new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

        val broadcastThresholds: BroadcastStream[ThresholdUpdate] = thresholds
            .broadcast(broadcastStateDescriptor)


        env.execute("UpdatableTempAlertApp")
    }
}
