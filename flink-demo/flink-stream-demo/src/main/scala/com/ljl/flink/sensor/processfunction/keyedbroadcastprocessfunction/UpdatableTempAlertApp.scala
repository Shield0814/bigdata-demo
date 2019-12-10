package com.ljl.flink.sensor.processfunction.keyedbroadcastprocessfunction

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, KeyedStream, StreamExecutionEnvironment}

case class ThresholdUpdate(id: String, threshold: Double)

/**
  * 需求：每个传感器的温度阈值可能不同，那么把每个传感器的超过阈值的温度作为广播流和实时传感器温度数据连接，
  * 如果某个传感器的温度连续2次超过它的 阈值那么进行报警
  */
object UpdatableTempAlertApp {

    def main(args: Array[String]): Unit = {

        val tool = ParameterTool.fromArgs(args)
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val sensorData: DataStream[SensorReading] = env.addSource(new SensorSource)
        val thresholds: DataStream[ThresholdUpdate] = env.fromCollection(
            Array(
                ThresholdUpdate("sensor_0", 1.0)
            )
        )

        if (tool.has("checkpoint")) {
            val checkpointDir = tool.get("checkpoint")
            //默认的checkpoint模式是exactly_once
            env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE)
            env.setStateBackend(new FsStateBackend(checkpointDir, true))
        }



        val keyedSensorData: KeyedStream[SensorReading, String] = sensorData.keyBy(_.id)
        // the descriptor of the broadcast state
        val broadcastStateDescriptor =
            new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

        val broadcastThresholds: BroadcastStream[ThresholdUpdate] = thresholds
            .broadcast(broadcastStateDescriptor)

        //键控流和广播流连接
        val keyedSensorDataWithThresholds: BroadcastConnectedStream[SensorReading, ThresholdUpdate] = keyedSensorData.connect(broadcastThresholds)

        val alerts: DataStream[(String, Double, Double)] = keyedSensorDataWithThresholds.process(new UpdatableTemperatureAlertFunction)

        alerts.print()

        env.execute("UpdatableTempAlertApp")
    }
}
