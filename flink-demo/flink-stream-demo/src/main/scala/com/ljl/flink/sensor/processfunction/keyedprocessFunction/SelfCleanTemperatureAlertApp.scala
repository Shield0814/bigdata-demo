package com.ljl.flink.sensor.processfunction.keyedprocessFunction

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 实现flink 状态的自动清除
  * 需求：如果某个传感器的温度在10s之内恢复到
  */
object SelfCleanTemperatureAlertApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

    }
}
