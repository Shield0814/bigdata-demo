package com.ljl.flink.sensor.processfunction.keyedprocessFunction

import com.ljl.flink.sensor.source.{SensorReading, SensorSource}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object TemperatureIncreaseAlertApp {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        //使用EventTime进行分析
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //设置Watermark生成周期
        env.getConfig.setAutoWatermarkInterval(500)

        //获取温度传感器数据，并分配时间戳和水位
        val sensorsData = env.addSource(new SensorSource).setParallelism(1)
            .assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(500)) {
                    override def extractTimestamp(r: SensorReading): Long = r.timestamp
                }
            )

        alertWhenTempInc(sensorsData)

        env.execute("TemperatureIncreaseAlertApp")


    }

    /**
      * 监控传感器温度数据，如果传感器温度上升，则进行报警提示
      *
      * @param sensorsData
      */
    def alertWhenTempInc(sensorsData: DataStream[SensorReading]) = {

        val tempIncAlert = sensorsData.keyBy(_.id)
            .process(new TemperatureIncAlertKeyedProcessFunction)
        tempIncAlert.print()

        //        sensorsData.keyBy(_.id)
        //            .flatMapWithState {
        //                case (in: SensorReading, None) =>
        //                    (List.empty[SensorReading], Some(in.temperature))
        //                case (in: SensorReading, Some(preTemp)) =>
        //                    if (math.abs(in.temperature - preTemp) > 1.7)
        //                        (List(in), Some(in.temperature))
        //                    else {
        //                        (List.empty[SensorReading], Some(in.temperature))
        //                    }
        //            }
    }


}
