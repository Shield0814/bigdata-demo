package com.ljl.flink.sensor.processfunction.keyedbroadcastprocessfunction

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.util.Collector

/**
  * 如果传感器两次温度的温差值大于等于报警规则流中设置的报警门槛值，则进行报警，
  * 该门槛值是可以更新的
  */
class UpdatableTemperatureAlertFunction
    extends KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)] {


    //广播状态的描述
    val thresholdsDesc = new MapStateDescriptor[String, Double]("thresholds", classOf[String], classOf[Double])

    var lastTemper: ValueState[Double] = null


    override def open(parameters: Configuration): Unit = {
        //初始化该传感器的上次温度值
        lastTemper = getRuntimeContext.getState(
            new ValueStateDescriptor[Double]("lastTemper", classOf[Double])
        )
    }

    /**
      * 按匹配规则，进行报警
      *
      * @param value
      * @param ctx
      * @param out
      */
    override def processElement(value: SensorReading,
                                ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#ReadOnlyContext,
                                out: Collector[(String, Double, Double)]): Unit = {
        //每次来一个数据时都重新获取广播状态，即获取报警规则【广播状态】
        val thresholds = ctx.getBroadcastState(thresholdsDesc)
        //获取上次传感器的温度
        val lastTemp = lastTemper.value()

        //如果规则流中存在该传感器，并且本次温度和上次温度差大于门槛值
        // 则进行报警输出:(传感器id，本次温度，和上次温度的温差)，否则啥都不做
        val tempDiff = Math.abs(value.temperature - lastTemp)
        if (thresholds.contains(value.id) && tempDiff > thresholds.get(value.id)) {
            out.collect((s"alert:${value.id}", value.temperature, tempDiff))
        }
        lastTemper.update(value.temperature)
    }

    /**
      * 处理广播流
      *
      * @param sensorThreshold
      * @param ctx
      * @param out
      */
    override def processBroadcastElement(sensorThreshold: ThresholdUpdate,
                                         ctx: KeyedBroadcastProcessFunction[String, SensorReading, ThresholdUpdate, (String, Double, Double)]#Context,
                                         out: Collector[(String, Double, Double)]): Unit = {
        val thresholds: BroadcastState[String, Double] = ctx.getBroadcastState(thresholdsDesc)
        if (sensorThreshold.threshold > 0.0d) {
            // 如果规则流中的传感器门槛值大于0，则更新传感器温度报警的温差值
            thresholds.put(sensorThreshold.id, sensorThreshold.threshold)
        } else {
            // 如果传感器温度门槛值小于等于0，则不对该传感器的温度进行报警
            thresholds.remove(sensorThreshold.id)
        }


    }
}
