package com.ljl.flink.stream.watermark


import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

case class SensorReading(id: String, temperature: Double, timestamp: Long)

object SenorEventTimeAppScala {

    def main(args: Array[String]): Unit = {
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        senv.setParallelism(1)
        println(senv.getConfig.getAutoWatermarkInterval)
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val readings = senv.socketTextStream("bigdata116", 9999)
            .map(info => {
                val fields = info.split(",").map(_.trim)
                SensorReading(fields(0), fields(1).toDouble, fields(2).toLong)
            }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(2)) {
            override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000
        })

        readings.print()
        //        val senorInfo = readings.keyBy("id")
        //            .timeWindow(Time.seconds(15), Time.seconds(5))
        //            //            .sideOutputLateData(OutputTag("late_data"))
        //            //            .min("temperature")
        //            .reduce((data1, data2) => SensorReading(data1.id, data1.timestamp, data1.temperature.min(data2.temperature)))
        //            .print()

        senv.execute("wartermark test")
    }

}
