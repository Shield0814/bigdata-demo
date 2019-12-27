package com.ljl.flink.stream.partitioner

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PartitionerApp {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val inputDataStream = env.socketTextStream("bigdata116", 9999)

        //GlobalPartitioner: 发往下游一个任务
        inputDataStream.global

        //forwardPartitioner
        inputDataStream.forward

        //RebalancePartitioner
        inputDataStream.rebalance

        //RescalePartitioner
        inputDataStream.rescale

        //KeyGroupPartitioner
        inputDataStream.keyBy("a")

        //BroadcastPartitioner
        inputDataStream.broadcast

        //ShufflePartitioner
        inputDataStream.shuffle

        //Custom
        //        inputDataStream.partitionCustom(new ShufflePartitioner[String](),x => x)
    }
}
