package com.ljl.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordcountByWindow {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordcountByWindow")
        val ssc = new StreamingContext(conf, Seconds(5))
        val lines = ssc.socketTextStream("bigdata116", 9999)
        lines.flatMap(_.split("\\W"))
            .map((_, 1))
            .reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Seconds(20), Seconds(5))
            .print()

    }
}
