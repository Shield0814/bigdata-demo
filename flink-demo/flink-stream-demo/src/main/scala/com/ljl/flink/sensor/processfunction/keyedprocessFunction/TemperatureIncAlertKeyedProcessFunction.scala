package com.ljl.flink.sensor.processfunction.keyedprocessFunction

import com.ljl.flink.sensor.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * KeyedProcessFunction
  * 监控温度传感器的温度值，如果温度值在 1s 之内(processing time)连续上升，报警
  * 1. &&& KeyedProcessFunction用来操作KeyedStream。&&&
  * 2. KeyedProcessFunction会处理流的每一个元素，输出为0个、1个或者多个元素。
  * 3. 所有的Process Function都继承自RichFunction接口，所以都有open()、close()和getRuntimeContext()等方法
  */
class TemperatureIncAlertKeyedProcessFunction extends KeyedProcessFunction[String, SensorReading, SensorReading] {


    //保存上次处理时某个传感器的温度信息
    lazy val lastSensorRecord: ValueState[SensorReading] = getRuntimeContext.getState(
        new ValueStateDescriptor[SensorReading]("lastTemp", Types.of[SensorReading])
    )

    //保存注册的报警的定时器时间戳，当processing time到达该时间时触发 onTimer方法的调用
    lazy val alertTimer: ValueState[Long] = getRuntimeContext.getState(
        new ValueStateDescriptor[Long]("alertTimer", Types.of[Long])
    )

    /**
      * 流中的每一个元素都会调用这个方法，调用结果将会放在Collector数据类型中输出。
      * Context可以访问元素的时间戳，元素的key，以及TimerService时间服务。
      * Context还可以将结果输出到别的流(side outputs)。
      *
      * @param value
      * @param ctx
      * @param out
      */
    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
        //上次传感器温度
        val prevTemp = lastSensorRecord.value() match {
            case SensorReading(_, _, temp) => temp
            case _ => Double.MaxValue
        }
        //更新上次传感器温度信息
        lastSensorRecord.update(value)

        //上次注册的报警时间
        val alertTime = alertTimer.value()

        if (prevTemp == 0.0 || value.temperature < prevTemp) {
            //如果本次传感器温度小于上次传感器温度，则删除报警定时器,并清空报警定时器事件
            //            ctx.timerService().deleteProcessingTimeTimer(alertTime)
            ctx.timerService().deleteEventTimeTimer(alertTime)
            alertTimer.clear()
        } else if (value.temperature - prevTemp > 5.0 && alertTime == 0) {
            //如果温度上升，并且还未注册报警定时器，则注册一个1s之后报警的报警定时器
            //            val alertProcessTime = ctx.timerService().currentProcessingTime() + 1000
            val alertProcessTime = ctx.timerService().currentWatermark() + 1000

            //            ctx.timerService().registerProcessingTimeTimer(alertProcessTime)
            ctx.timerService().deleteEventTimeTimer(alertProcessTime)
            alertTimer.update(alertProcessTime)
        }
        //        println(prevTemp)
        //输出处理后的结果：温度上升多少
        out.collect(SensorReading(value.id, value.timestamp, value.temperature - prevTemp))

    }

    /**
      * 当之前注册的定时器触发时调用。参数timestamp为定时器所设定的触发的时间戳。
      * Collector为输出结果的集合。
      * OnTimerContext和processElement的Context参数一样，提供了上下文的一些信息，
      * 例如firing trigger的时间信息(事件时间或者处理时间)
      *
      * @param timestamp
      * @param ctx
      * @param out
      */
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
        //打印报警信息
        out.collect(SensorReading(s"WARNING:传感器id为:${ctx.getCurrentKey}的温度已连续上升1s了...", 0, 0))
        //报警之后清除报警时间戳，以备下次报警
        alertTimer.clear()
    }
}