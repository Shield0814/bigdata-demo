package com.ljl.flink.sensor.processfunction.windowfunction.aggregatefunction

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * 增量计算每个传感器的平均温度
  */
class AvgTemperatureAggregateFunction extends AggregateFunction[SensorReading, (String, Double, Long), (String, Double)] {

    override def createAccumulator(): (String, Double, Long) = ("", 0.0, 0)

    override def add(v: SensorReading, acc: (String, Double, Long)): (String, Double, Long) = {
        (v.id, acc._2 + v.temperature, acc._3 + 1)
    }

    override def getResult(acc: (String, Double, Long)): (String, Double) = (acc._1, acc._2 / acc._3)


    override def merge(a: (String, Double, Long), b: (String, Double, Long)): (String, Double, Long) = {
        println(a._1 + "_" + b._1)
        (a._1, a._2 + b._2, a._3 + b._3)
    }
}
