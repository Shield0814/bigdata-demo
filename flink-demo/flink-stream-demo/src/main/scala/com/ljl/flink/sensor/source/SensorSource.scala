package com.ljl.flink.sensor.source

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
  * 温度传感器数据样例类
  *
  * @param id          传感器id
  * @param timestamp   数据产生的时间戳
  * @param temperature 传感器温度
  */
case class SensorReading(id: String, timestamp: Long, temperature: Double)

/**
  * 模拟产生温度传感器数据
  */
class SensorSource extends RichParallelSourceFunction[SensorReading] {

    private val random = new Random
    //source是否产生数据，如果为false表示停止产生数据
    @volatile var running = true

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

        //并行source中，当前子任务的任务索引
        val taskIdx = getRuntimeContext.getIndexOfThisSubtask + 1

        //随机生成10个传感器id和温度
        var tmpTemporature = for (i <- 0 until 1)
            yield (s"sensor_${i}", 65 + (random.nextGaussian() * 20))

        while (running) {
            //更新以上随机产生的传感器温度
            tmpTemporature = tmpTemporature.map(stemp => (stemp._1, stemp._2 + random.nextGaussian() * 0.5))

            //当前系统时间所谓传感器温度数据产生的时间
            var currentTimeMills = System.currentTimeMillis()

            //对传感器数据产生时间做一个随机1000ms以内的延迟,
            tmpTemporature.map(stemp => SensorReading(stemp._1, currentTimeMills - random.nextInt(3000), stemp._2))
                .foreach(ctx.collect(_))

            //每个100ms发送一次数据
            Thread.sleep(500)
        }
    }

    override def cancel(): Unit = running = false


}
