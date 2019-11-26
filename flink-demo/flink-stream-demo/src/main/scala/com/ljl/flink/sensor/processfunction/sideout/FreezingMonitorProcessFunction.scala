package com.ljl.flink.sensor.processfunction.sideout

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector


/**
  * 侧向输出
  * 可用于迟到数据输出到其他地方
  * 冰点监控器：如果传感器温度低于30度，则报警输出该传感器的信息到一个侧向输出流中
  */
class FreezingMonitorProcessFunction extends ProcessFunction[SensorReading, SensorReading] {

    lazy val lateRecord = new OutputTag[SensorReading]("late-record")


    lazy val freezingAlert: OutputTag[(String, SensorReading)] = new OutputTag[(String, SensorReading)]("freezing-alarms")

    override def processElement(value: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
        if (value.temperature < 30.0) {
            //使用侧向输出 side output
            ctx.output(freezingAlert, (s"传感器${value.id}温度过低，可能已损坏", value))
        }
        if (value.timestamp < ctx.timerService().currentWatermark()) {
            //如果事件事件小于当前水位，说明该元素迟到，输出到迟到元素侧向输出流中
            ctx.output(lateRecord, value)
        }
        out.collect(value)

    }
}
