package com.ljl.flink.stream.transformations

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object TransformationAppScala {

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //        datastream和splitstream互转
        //        dataStream2SplitStream(env)

        //        datastream和keyedStream互转
        //        dataStream2KeyedStream(env)
        keyedStreamFunctions(env)

        env.execute("transformation")
    }


    /**
      * keyedStream相关算子操作
      *
      * @param env
      */
    def keyedStreamFunctions(env: StreamExecutionEnvironment) = {
        val wcKeyedStream: KeyedStream[(String, Int), Tuple] = env.socketTextStream("bigdata116", 9999).setParallelism(1)
            .flatMap(_.split("\\W"))
            .map((_, 1))
            .keyBy(0)
        val value: KeyedStream[(String, Int), String] = env.socketTextStream("bigdata116", 9999)
            .flatMap(_.split("\\W"))
            .map((_, 1))
            .keyBy(_._1)
        wcKeyedStream.print.setParallelism(1)
    }


    /**
      * datastream和keyedStream互转
      * keyedStream是滚动聚合的，keyedStream一般是配合窗口使用的
      *
      * @param env
      * @return
      */
    def dataStream2KeyedStream(env: StreamExecutionEnvironment) = {
        val dataStream: DataStream[(Int, Int)] = env.fromCollection(Array(1, 2, 3, 2, 1, 1)).map((_, 1))
        val keyedStream: KeyedStream[(Int, Int), Tuple] = dataStream.keyBy(0)
        val datastream2: DataStream[(Int, Int)] = keyedStream.sum(1)
        datastream2.print.setParallelism(1)
    }

    /**
      * datastream和splitstream互转
      * 一般情况下select配合split使用
      *
      * @param env
      * @return
      */
    def dataStream2SplitStream(env: StreamExecutionEnvironment) = {
        val dataStream: DataStream[Int] = env.fromCollection(Array(1, 2, 3, 4, 5, 6))
        val splitStream = dataStream.split(v => {
            (v % 2) match {
                case 0 => List("even")
                case 1 => List("odd")
            }
        })

        val dataStream2: DataStream[Int] = splitStream.select("odd")
        dataStream2.print().setParallelism(1)
    }

}
