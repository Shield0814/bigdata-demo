package com.ljl.flink.stream.window

import org.apache.flink.api.common.functions.ReduceFunction

/**
  * window function 之自定义ReduceFunction
  */
class CustomSumReduceFunction extends ReduceFunction[Int] {

    override def reduce(value1: Int, value2: Int): Int = value1 + value2
}
