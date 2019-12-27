package com.ljl.flink.sensor

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * Flink提供了8个Process Function：
  * ProcessFunction
  * KeyedProcessFunction
  * CoProcessFunction
  * ProcessJoinFunction
  * BroadcastProcessFunction
  * KeyedBroadcastProcessFunction
  * ProcessWindowFunction
  * ProcessAllWindowFunction
  */
object SensorAppDemo {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        //使用EventTime进行分析
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //设置产生水位线watermark的周期,默认200ms
        env.getConfig.setAutoWatermarkInterval(2000)

        val sensorData: DataStream[SensorReading] = env.addSource(new SensorSource).setParallelism(1)


        //根据传感器温度把传感器切分为 NORMAL[<90],WARN[>=90,<100],DANGER[>=100]三个SplitStream
        val sensorDataWithState: SplitStream[SensorReading] = sensorData.split(r => {
            if (r.temperature >= 100) {
                List("DANGER")
            } else if (r.temperature < 100 && r.temperature >= 90) {
                List("WARN")
            } else {
                List("NORMAL")
            }
        })


        //输出WARN 传感器的最大温度
        //        warnSensorsMax(sensorDataWithState)

        //每隔2s输出每个危险状态传感器最近10s的最大温度
        dangerSensorsMinTemperature(sensorDataWithState)

        env.execute("SensorAppDemo")
    }

    /**
      * 关注处于危险状态的传感器，每隔2s输出每个危险状态传感器最近10s的最大温度
      *
      * @param sensorDataWithState
      */
    def dangerSensorsMinTemperature(sensorDataWithState: SplitStream[SensorReading]) = {
        val idKeyedStream: KeyedStream[SensorReading, String] = sensorDataWithState.select("DANGER")
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(500)) {
                    override def extractTimestamp(element: SensorReading): Long = element.timestamp
                })
            .keyBy(_.id)
        val windowedStream: WindowedStream[SensorReading, String, TimeWindow] = idKeyedStream
            .timeWindow(Time.seconds(10), Time.seconds(2))
        windowedStream.reduce((r1, r2) => SensorReading(r1.id, r1.timestamp, r1.temperature.max(r2.temperature)))
            .print()

    }

    /**
      * 找出警告传感器中每个传感器的最大温度，最小温度，平均温度
      *
      * @param sensorDataWithState
      */
    def warnSensorsMax(sensorDataWithState: SplitStream[SensorReading]) = {
        val warnSensors: DataStream[(String, Double)] = sensorDataWithState
            .select("WARN")
            .map(r => (r.id, r.temperature))
        //警告传感器的最大温度
        val warnSensorsMaxTemper = warnSensors.keyBy(r => r._1)
            .max(1)
        warnSensorsMaxTemper.print().setParallelism(1)
    }
}
