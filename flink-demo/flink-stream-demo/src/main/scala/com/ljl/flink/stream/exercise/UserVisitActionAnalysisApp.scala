package com.ljl.flink.stream.exercise

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._


case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: Long,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

/**
  * 品类维度统计信息
  *
  * @param categoryId 品类id
  * @param clickCnt   点击次数
  * @param orderCnt   下单次数
  * @param payCnt     支付次数
  */
case class CategoryStatInfo(categoryId: Long, clickCnt: Long, orderCnt: Long, payCnt: Long)


object UserVisitActionAnalysisApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val path = "D:\\projects\\bigdata-demo\\flink-demo\\flink-stream-demo\\src\\main\\resources\\user_visit_action.txt"
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        //用户访问行为Datastream
        val userVisitActions: DataStream[UserVisitAction] = env.readTextFile(path).map(line => {
            val fields = line.split("_")
            val sps = line.split("_")
            UserVisitAction(sps(0), sps(1).toLong, sps(2), sps(3).toLong,
                sdf.parse(sps(4)).getTime, sps(5), sps(6).toLong, sps(7).toLong,
                sps(8), sps(9), sps(10), sps(11), sps(12).toLong)
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserVisitAction](Time.milliseconds(100L)) {
            override def extractTimestamp(element: UserVisitAction): Long = element.action_time
        })

        //需求一: 每个一天统计最近一天数据中top10热门品类
        computeTop10HotCategories(userVisitActions).print()

        env.execute("UserVisitActionAnalysisApp")
    }


    def computeTop10HotCategories2(userVisitActions: DataStream[UserVisitAction], env: ExecutionEnvironment) = {
        val categoryInfoDataStream: DataStream[CategoryStatInfo] = userVisitActions.flatMap(uva => {
            uva match {
                case uva: UserVisitAction if uva.click_category_id != -1 =>
                    Iterator(CategoryStatInfo(uva.click_category_id, 1L, 0L, 0L))
                case uva: UserVisitAction if uva.order_category_ids != "null" =>
                    uva.order_category_ids.split(",")
                        .map(categoryId => CategoryStatInfo(categoryId.toLong, 0L, 1L, 0L))
                        .toIterator
                case uva: UserVisitAction if uva.pay_category_ids != "null" =>
                    uva.pay_category_ids.split(",")
                        .map(categoryId => CategoryStatInfo(categoryId.toLong, 0L, 0L, 1L))
                        .toIterator
                case _ =>
                    Iterator()
            }
        })

    }

    /**
      * 每天统计top10热门品类
      *
      * @param userVisitActions 用户访问行为数据流
      */
    def computeTop10HotCategories(userVisitActions: DataStream[UserVisitAction]) = {

        //1. 统计每个品类的点击次数，下单次数，支付次数
        //categoryId,clickCount,orderCount,payCount
        //1.1 准备统计品类点击次数，下单次数，支付次数的数据
        val categoryInfoDataStream: DataStream[CategoryStatInfo] = userVisitActions.flatMap(uva => {
            uva match {
                case uva: UserVisitAction if uva.click_category_id != -1 =>
                    Iterator(CategoryStatInfo(uva.click_category_id, 1L, 0L, 0L))
                case uva: UserVisitAction if uva.order_category_ids != "null" =>
                    uva.order_category_ids.split(",")
                        .map(categoryId => CategoryStatInfo(categoryId.toLong, 0L, 1L, 0L))
                        .toIterator
                case uva: UserVisitAction if uva.pay_category_ids != "null" =>
                    uva.pay_category_ids.split(",")
                        .map(categoryId => CategoryStatInfo(categoryId.toLong, 0L, 0L, 1L))
                        .toIterator
                case _ =>
                    Iterator()
            }
        })

        //1.2 按品类聚合统计点击次数，下单次数，支付次数,并关联window结束时间
        val categoryStatInfoDataStream = categoryInfoDataStream.keyBy(_.categoryId)
            .timeWindow(Time.days(1000))
            .reduce(new ReduceFunction[CategoryStatInfo] {
                override def reduce(info1: CategoryStatInfo, info2: CategoryStatInfo): CategoryStatInfo = {
                    CategoryStatInfo(info1.categoryId,
                        info1.clickCnt + info2.clickCnt,
                        info1.orderCnt + info2.orderCnt,
                        info1.payCnt + info2.payCnt
                    )
                }
            }, new ProcessWindowFunction[CategoryStatInfo, (Long, CategoryStatInfo), Long, TimeWindow] {
                override def process(key: Long,
                                     context: Context,
                                     elements: Iterable[CategoryStatInfo],
                                     out: Collector[(Long, CategoryStatInfo)]): Unit = {
                    val info = elements.iterator.next()
                    out.collect((context.window.getEnd, info))
                }
            })

        //2. 以window结束时间为key，计算每个窗口中的top10品类
        categoryStatInfoDataStream.keyBy(_._1)
            .process(new KeyedProcessFunction[Long, (Long, CategoryStatInfo), (Int, CategoryStatInfo)] {

                //保存topN的信息
                var topNListState: ListState[CategoryStatInfo] = _

                //输出结果的时间：window结束时间 + 1毫秒
                var outputTimerValue: ValueState[Long] = _

                override def open(parameters: Configuration): Unit = {
                    topNListState = getRuntimeContext.getListState(
                        new ListStateDescriptor[CategoryStatInfo]("topNListState", classOf[CategoryStatInfo])
                    )
                    outputTimerValue = getRuntimeContext.getState(
                        new ValueStateDescriptor[Long]("outputTimerValue", classOf[Long])
                    )

                }

                override def processElement(value: (Long, CategoryStatInfo),
                                            ctx: KeyedProcessFunction[Long, (Long, CategoryStatInfo), (Int, CategoryStatInfo)]#Context,
                                            out: Collector[(Int, CategoryStatInfo)]): Unit = {
                    //没来一个数据加入到TopNListState中并重新计算topN
                    topNListState.add(value._2)
                    val statInfos = topNListState.get()
                    if (statInfos.size > 10) {
                        val afterSort = statInfos.toList.sortWith(sortCategoryStatInfo(_, _))
                        topNListState.clear()
                        topNListState.addAll(afterSort.init)
                    }
                    //注册一个定时器：在事件事件到达window结束事件后输出topN数据
                    val outputTime = outputTimerValue.value()
                    if (outputTime == 0 || ctx.timestamp() > outputTime) {
                        outputTimerValue.update(value._1 + 1)
                        ctx.timerService().registerEventTimeTimer(value._1 + 1)
                    }

                }

                override def onTimer(timestamp: Long,
                                     ctx: KeyedProcessFunction[Long, (Long, CategoryStatInfo), (Int, CategoryStatInfo)]#OnTimerContext,
                                     out: Collector[(Int, CategoryStatInfo)]): Unit = {
                    val topNInfos = topNListState.get().toList.sortWith(sortCategoryStatInfo(_, _))
                    println("#######" + topNInfos.size)
                    println("$$$$$$$$$" + outputTimerValue.value())
                    (1 to 10).zip(topNInfos)
                        .foreach(out.collect)
                    outputTimerValue.clear()
                    topNListState.clear()

                }

                /**
                  * 对两个品类进行比较,如果l>r返回true，否则返回false
                  *
                  * @param l
                  * @param r
                  * @return
                  */
                def sortCategoryStatInfo(l: CategoryStatInfo, r: CategoryStatInfo): Boolean = {
                    if (l.clickCnt > r.clickCnt) true
                    else if (l.clickCnt == r.clickCnt)
                        if (l.orderCnt > r.orderCnt) true
                        else if (l.orderCnt == r.orderCnt)
                            if (l.payCnt > r.payCnt) true else false
                        else false
                    else false
                }
            })

    }


}
