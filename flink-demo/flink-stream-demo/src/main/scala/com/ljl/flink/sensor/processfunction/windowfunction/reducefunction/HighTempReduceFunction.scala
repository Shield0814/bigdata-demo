package com.ljl.flink.sensor.processfunction.windowfunction.reducefunction

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction

class HighTempReduceFunction extends ReduceFunction[SensorReading] {

    override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
        if (value1.temperature > value2.temperature) value1
        else value2
    }
}
