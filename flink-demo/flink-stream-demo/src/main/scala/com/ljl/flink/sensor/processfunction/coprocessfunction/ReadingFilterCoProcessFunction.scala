package com.ljl.flink.sensor.processfunction.coprocessfunction

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector

/**
  * CoProcessFunction:
  * 对于两条输入流，DataStream API提供了CoProcessFunction这样的low-level操作。
  * CoProcessFunction提供了操作每一个输入流的方法: processElement1()和processElement2()。
  *
  * 需求：通过stream2通知stream1的输出,
  */
class ReadingFilterCoProcessFunction extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {

    //决定数据是否转发
    lazy val forwardingEnabled: ValueState[Boolean] = getRuntimeContext.getState(
        new ValueStateDescriptor[Boolean]("filterSwitch", Types.of[Boolean])
    )

    lazy val disabledTimer: ValueState[Long] = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("disableTimer", Types.of[Long])
    )


    /**
      * 处理传感器数据Datastream是否输出到下一步
      *
      * @param value
      * @param ctx
      * @param out
      */
    override def processElement1(value: SensorReading,
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
        if (forwardingEnabled.value()) {
            out.collect(value)
        }

    }

    /**
      * 根据该流控制stream1是否打印
      *
      * @param switch
      * @param ctx
      * @param out
      */
    override def processElement2(switch: (String, Long),
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
        //如果stream2中进入一个元素，那么开启stream1输出，并添加一个定时器
        forwardingEnabled.update(true)

        val timerTimestamp = ctx.timerService().currentProcessingTime() + switch._2
        val curTimerTimestamp = disabledTimer.value()

        if (timerTimestamp > curTimerTimestamp) {

            ctx.timerService().deleteEventTimeTimer(curTimerTimestamp)
            ctx.timerService().registerProcessingTimeTimer(timerTimestamp)
            disabledTimer.update(timerTimestamp)
        }

    }

    override def onTimer(timestamp: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
        forwardingEnabled.clear()
        disabledTimer.clear()
    }
}
