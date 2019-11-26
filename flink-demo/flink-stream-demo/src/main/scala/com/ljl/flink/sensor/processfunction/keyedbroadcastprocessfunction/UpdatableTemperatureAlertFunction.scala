package com.ljl.flink.sensor.processfunction.keyedbroadcastprocessfunction

import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

class UpdatableTemperatureAlertFunction extends KeyedBroadcastProcessFunction {
    override def processElement(value: Nothing, ctx: KeyedBroadcastProcessFunction[Nothing, Nothing, Nothing, Nothing]#ReadOnlyContext, out: Collector[Nothing]): Unit = ???

    override def processBroadcastElement(value: Nothing, ctx: KeyedBroadcastProcessFunction[Nothing, Nothing, Nothing, Nothing]#Context, out: Collector[Nothing]): Unit = ???
}
