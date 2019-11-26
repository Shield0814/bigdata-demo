package com.ljl.flink.stream.time


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object EventTimeAppScala {

    def main(args: Array[String]): Unit = {
        val senv = StreamExecutionEnvironment.getExecutionEnvironment
        //Event产生的时间
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        //注入flink的时间
        senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
        //默认时Process Time
        senv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    }
}
