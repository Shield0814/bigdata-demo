package com.ljl.flink.stream.join

import org.apache.flink.api.common.functions.RichJoinFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class OrderInfo(id: String, ts: Long)

case class OrderDetail(id: String, orderId: String, ts: Long)

object OrderInfoAndOrderDetailJoinApp {


    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


        val orderInfoDataStream = env.socketTextStream("bigdata116", 6666)
            .map(orderInfo => {
                val fields = orderInfo.split(",")
                OrderInfo(fields(0), fields(1).trim.toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderInfo](Time.seconds(5)) {
                override def extractTimestamp(element: OrderInfo): Long = element.ts * 1000L
            }).keyBy(_.id)

        orderInfoDataStream.print().setParallelism(1)

        val orderDetailDataStream = env.socketTextStream("bigdata116", 9999)
            .map(detailInfo => {
                val fields = detailInfo.split(",")
                OrderDetail(fields(0), fields(1), fields(2).trim.toLong)
            })
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderDetail](Time.seconds(5)) {
                override def extractTimestamp(element: OrderDetail): Long = element.ts * 1000L
            }).keyBy(_.orderId)

        orderDetailDataStream.print().setParallelism(1)
        val joinedDataStream = orderInfoDataStream.join[OrderDetail](orderDetailDataStream)
            .where(_.id)
            .equalTo(_.orderId)
            .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
            .apply(new RichJoinFunction[OrderInfo, OrderDetail, (String, String, Long, Long)] {
                override def join(first: OrderInfo, second: OrderDetail): (String, String, Long, Long) = {
                    (s"orderid:${first.id}", s"detailid:${second.id}", first.ts, second.ts)
                }
            })
        joinedDataStream.print().setParallelism(1)

        env.execute("OrderInfoAndOrderDetailJoinApp")
    }
}
