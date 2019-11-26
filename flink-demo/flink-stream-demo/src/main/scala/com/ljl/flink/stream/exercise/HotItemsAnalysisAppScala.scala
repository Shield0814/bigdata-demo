package com.ljl.flink.stream.exercise

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItemsAnalysisAppScala {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val filePath = "D:\\projects\\bigdata-demo\\user-behavior-analysis\\hot-items-analysis\\src\\main\\resources\\user-behaviors.csv"


        //需求一：每隔 5min 统计最近 1hour 内pv次数最多的3个产品
        val userBehaviors = env.readTextFile(filePath)
            .map(line => {
                val splits = line.split(",")
                UserBehavior(splits(0).toInt, splits(1).toLong,
                    splits(2).toInt, splits(3), splits(4).toLong * 1000)
            }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
            override def extractTimestamp(element: UserBehavior): Long = element.timestamp
        })
        computeTopNItems(3, userBehaviors).print()


        env.execute("HotItemsAnalysisAppScala")
    }


    /**
      * 每个 5min 计算近 1hour 的 topN 商品
      *
      * @param n
      * @param userBehaviors
      */
    def computeTopNItems(n: Int, userBehaviors: DataStream[UserBehavior]) = {
        //近1小时窗口中每个商品的pv次数
        val itemViewCount = userBehaviors.filter(_.behavior == "pv").keyBy(_.itemId)
            .timeWindow(Time.seconds(10), Time.seconds(5))
            .aggregate(new AggregateFunction[UserBehavior, Long, Long] {
                override def createAccumulator(): Long = 0L

                override def add(v: UserBehavior, acc: Long): Long = acc + 1L

                override def getResult(acc: Long): Long = acc

                override def merge(a: Long, b: Long): Long = a + b
            }, new ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
                override def process(key: Long,
                                     context: Context,
                                     elements: Iterable[Long],
                                     out: Collector[ItemViewCount]): Unit = {
                    out.collect(ItemViewCount(key, context.window.getEnd, elements.iterator.next()))
                }
            })


        //计算topN
        itemViewCount.keyBy(_.windowEnd)
            .process(new KeyedProcessFunction[Long, ItemViewCount, String] {

                //保存viewcount最大的n个商品的信息
                var topNListState: ListState[ItemViewCount] = _

                //触发topNListState输出并清除的定时器的值
                var outputTimeState: ValueState[Long] = _

                /**
                  * 初始化
                  *
                  * @param parameters
                  */
                override def open(parameters: configuration.Configuration): Unit = {
                    topNListState = getRuntimeContext.getListState(
                        new ListStateDescriptor[ItemViewCount]("topNListState", classOf[ItemViewCount])
                    )
                    outputTimeState = getRuntimeContext.getState(
                        new ValueStateDescriptor[Long]("outputTimeState", classOf[Long])
                    )
                }


                override def processElement(value: ItemViewCount,
                                            ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                                            out: Collector[String]): Unit = {

                    topNListState.add(value)
                    val topNItems = topNListState.get()
                    if (topNItems.size > n) {
                        topNListState.clear()
                        topNListState.update(topNItems.toList.sortWith(_.count > _.count).init)
                    }

                    //注册一个window结束时间的定时器
                    if (outputTimeState.value() == 0L
                        || outputTimeState.value() < ctx.timerService().currentProcessingTime()) {
                        outputTimeState.update(value.windowEnd + 1)
                        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
                    }

                }

                override def onTimer(timestamp: Long,
                                     ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                                     out: Collector[String]): Unit = {
                    val sortedItems = topNListState.get().toList.sortWith(_.count > _.count)

                    val result: StringBuilder = new StringBuilder
                    result.append("====================================\n")
                    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
                    for (i <- sortedItems.indices) {
                        val currentItem: ItemViewCount = sortedItems(i)
                        // e.g.  No1：  商品ID=12224  浏览量=2413
                        result.append("No").append(i + 1).append(":")
                            .append("  商品ID=").append(currentItem.itemId)
                            .append("  浏览量=").append(currentItem.count).append("\n")
                    }
                    result.append("====================================\n\n")
                    Thread.sleep(1000)
                    out.collect(result.toString)
                }
            })
    }


}
