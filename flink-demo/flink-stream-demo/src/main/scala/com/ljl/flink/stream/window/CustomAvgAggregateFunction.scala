package com.ljl.flink.stream.window

import org.apache.flink.api.common.functions.AggregateFunction

/**
  * window function之自定义聚合函数，求平均值，类似于spark的aggregate算子
  *
  */
class CustomAvgAggregateFunction extends AggregateFunction[(String, Int), (Int, Int), Double] {

    override def createAccumulator(): (Int, Int) = (0, 0)

    override def add(value: (String, Int), acc: (Int, Int)): (Int, Int) = (acc._1 + value._2, acc._2 + 1)

    override def getResult(accumulator: (Int, Int)): Double = accumulator._1.toDouble / accumulator._2

    override def merge(a: (Int, Int), b: (Int, Int)): (Int, Int) = (a._1 + b._1, a._2 + b._2)
}
